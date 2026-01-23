from moviebot.bot.bot_init import bot
"""
Callback handlers для карточки фильма (add_to_database, plan_from_added, show_facts и т.д.)
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
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
    """Обработчик кнопки '➕ Добавить в базу' — добавляет фильм/сериал и показывает актуальную карточку"""
    logger.info(f"[ADD TO DB] START: data={call.data}, user={call.from_user.id}")

    chat_id = call.message.chat.id
    user_id = call.from_user.id
    message_id = call.message.message_id
    thread_id = getattr(call.message, 'message_thread_id', None)

    try:
        bot.answer_callback_query(call.id, text="⏳ Добавляю в базу...")

        # 1. Парсим kp_id
        kp_id_str = call.data.split(":", 1)[1].strip()
        try:
            kp_id = int(kp_id_str)
        except ValueError:
            logger.error(f"[ADD TO DB] Некорректный kp_id: {kp_id_str}")
            bot.edit_message_text("❌ Некорректный ID", chat_id, message_id, message_thread_id=thread_id)
            return

        # 2. Пытаемся взять максимально свежие данные из API
        from moviebot.api.kinopoisk_api import extract_movie_info

        link = f"https://www.kinopoisk.ru/film/{kp_id}/"  # начальная
        info = extract_movie_info(link)

        if not info or not info.get('title'):
            logger.warning("[ADD TO DB] API не вернул данные → fallback")
            info = {}  # будем пытаться восстановить минимально

        is_series = info.get('is_series', False)

        # Корректируем ссылку в зависимости от типа
        if is_series:
            link = f"https://www.kinopoisk.ru/series/{kp_id}/"

        # 3. Проверяем, вдруг уже есть в базе
        existing = None
        # Используем локальные соединение и курсор
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        
        with db_lock:
            try:
                cursor_local.execute("""
                    SELECT id, title, watched 
                    FROM movies 
                    WHERE chat_id = %s AND kp_id = %s
                """, (chat_id, kp_id_str))
                row = cursor_local.fetchone()

                if row:
                    film_id = row.get('id') if isinstance(row, dict) else row[0]
                    title_db = row.get('title') if isinstance(row, dict) else row[1]
                    watched = row.get('watched') if isinstance(row, dict) else row[2]
                    existing = (film_id, title_db, watched)
                    logger.info(f"[ADD TO DB] Уже существует → existing={existing}")
                    conn_local.commit()
                else:
                    # 4. Добавляем в базу
                    title = info.get('title', f"Без названия {kp_id}")
                    year = info.get('year')
                    genres = info.get('genres')
                    description = info.get('description')
                    director = info.get('director')
                    actors = info.get('actors')

                    cursor_local.execute('''
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

                    result = cursor_local.fetchone()
                    if result:
                        film_id = result.get('id') if isinstance(result, dict) else result[0]
                        title_db = result.get('title') if isinstance(result, dict) else result[1]
                        watched = result.get('watched') if isinstance(result, dict) else result[2]
                        existing = (film_id, title_db, watched)
                    conn_local.commit()

                    logger.info(f"[ADD TO DB] Добавлен/обновлён → existing={existing}")

            except Exception as db_err:
                logger.error(f"[ADD TO DB] Ошибка БД: {db_err}", exc_info=True)
                try:
                    conn_local.rollback()
                except:
                    pass
                raise

        # 5. Гарантируем наличие is_series в info
        if 'is_series' not in info:
            # Последняя проверка по ссылке (самый надёжный fallback)
            info['is_series'] = '/series/' in link
            logger.warning(f"[ADD TO DB] is_series отсутствовал в info → восстановили по ссылке: {info['is_series']}")

        # 6. Финальная карточка — используем get_film_current_state для актуального состояния
        from moviebot.bot.handlers.series import get_film_current_state, show_film_info_with_buttons
        # Получаем актуальное состояние после добавления
        current_state = get_film_current_state(chat_id, kp_id, user_id)
        actual_existing = current_state['existing']
        
        show_film_info_with_buttons(
            chat_id=chat_id,
            user_id=user_id,
            info=info,                  # теперь is_series точно есть
            link=link,
            kp_id=kp_id,
            existing=actual_existing,   # Используем актуальное состояние
            message_id=message_id,
            message_thread_id=thread_id
        )

        bot.answer_callback_query(call.id, "✅ Готово!", show_alert=False)

    except Exception as e:
        logger.error(f"[ADD TO DB] Критическая ошибка: {e}", exc_info=True)
        try:
            conn_local = get_db_connection()
            conn_local.rollback()
        except:
            pass
        try:
            bot.edit_message_text("❌ Не удалось добавить в базу", chat_id, message_id, message_thread_id=thread_id)
        except:
            pass

    logger.info("[ADD TO DB] END")

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
            conn_check = get_db_connection()
            cursor_check = get_db_cursor()
            with db_lock:
                cursor_check.execute(
                    'SELECT title, link, is_series FROM movies WHERE chat_id = %s AND kp_id = %s',
                    (chat_id, str(kp_id))
                )
                row = cursor_check.fetchone()
                if row:
                    title = row.get('title') if isinstance(row, dict) else row[0]
                    link = row.get('link') if isinstance(row, dict) else row[1]
                    is_series = bool(row.get('is_series') if isinstance(row, dict) else row[2])
                    logger.info(f"[PLAN FROM ADDED] Название взято из базы: {title}")
        except Exception as db_e:
            logger.error(f"[PLAN FROM ADDED] Ошибка чтения из БД: {db_e}", exc_info=True)
            title = None
            link = None
            is_series = False
                
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
            # Используем локальные соединение и курсор
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            
            with db_lock:
                try:
                    # Проверяем наличие + сразу берём нужные поля
                    cursor_local.execute(
                        'SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s',
                        (chat_id, str(kp_id))
                    )
                    row = cursor_local.fetchone()

                    if row:
                        existing = row  # row может быть dict или tuple
                        film_id, watched = extract_film_info_from_existing(existing)
                        logger.info(f"[PLAN FROM ADDED] Фильм уже в базе: film_id={film_id}, watched={watched}")
                    else:
                        # Добавляем новый
                        is_series_int = 1 if is_series else 0
                        cursor_local.execute('''
                            INSERT INTO movies (chat_id, kp_id, title, link, is_series, added_by, added_at, source)
                            VALUES (%s, %s, %s, %s, %s, %s, NOW(), 'plan_button')
                            ON CONFLICT (chat_id, kp_id) DO NOTHING
                            RETURNING id, title, watched
                        ''', (chat_id, str(kp_id), title, link, is_series_int, user_id))
                        
                        result = cursor_local.fetchone()
                        if result:
                            existing = result
                            film_id, watched = extract_film_info_from_existing(existing)
                            logger.info(f"[PLAN FROM ADDED] Фильм добавлен: film_id={film_id}")
                        
                        conn_local.commit()
                except Exception as db_e:
                    logger.error(f"[PLAN FROM ADDED] Ошибка при работе с БД: {db_e}", exc_info=True)
                    try:
                        conn_local.rollback()
                    except:
                        pass
                    raise
        except Exception as db_e:
            logger.error(f"[PLAN FROM ADDED] Ошибка БД при добавлении фильма: {db_e}", exc_info=True)
            try:
                conn_local = get_db_connection()
                conn_local.rollback()
            except:
                pass
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
    """Обработчик кнопки 'Факты'"""
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
        # ВАЖНО: Используем локальные соединения вместо глобальных
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        
        try:
            with db_lock:
                cursor_local.execute('''
                    SELECT id, title, watched, link 
                    FROM movies 
                    WHERE chat_id = %s AND kp_id = %s
                ''', (chat_id, str(kp_id)))
                row = cursor_local.fetchone()
                
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
        except Exception as db_e:
            logger.error(f"[PLAN TYPE] Ошибка БД: {db_e}", exc_info=True)
            try:
                conn_local.rollback()
            except:
                pass
            bot.send_message(chat_id, "❌ Ошибка при получении данных из базы. Попробуйте заново.")
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

        # Отправляем промпт и сразу сохраняем его message_id
        sent_prompt = bot.send_message(
            chat_id,
            "📅 Когда планируете смотреть?\n\nПримеры:\n"
            "• сегодня\n"
            "• завтра 20:00\n"
            "• 15.01\n"
            "• понедельник вечером\n"
            "• 17 января 21:00",
            parse_mode='HTML'
        )

        # Самое важное — сохраняем ID промпта!
        user_plan_state[user_id]['prompt_message_id'] = sent_prompt.message_id
        logger.info(f"[PLAN TYPE] Сохранён prompt_message_id={sent_prompt.message_id} для user {user_id}")
        
        
    except Exception as e:
        logger.error(f"[PLAN TYPE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка, попробуйте заново.", show_alert=True)
        except Exception as e:
            logger.warning(f"[CALLBACK] Не удалось ответить на callback: {e}")



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
        # ВАЖНО: Используем локальные соединения вместо глобальных
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        
        try:
            with db_lock:
                cursor_local.execute('''
                    SELECT id, title, watched, link, kp_id, year, genres, description, director, actors, is_series
                    FROM movies WHERE id = %s AND chat_id = %s
                ''', (film_id, chat_id))
                row = cursor_local.fetchone()
                
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
                cursor_local.execute('UPDATE movies SET watched = 1 WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                conn_local.commit()
                logger.info(f"[MARK WATCHED] Фильм {film_id} отмечен как просмотренный")
        except Exception as db_e:
            logger.error(f"[MARK WATCHED] Ошибка БД: {db_e}", exc_info=True)
            try:
                conn_local.rollback()
            except:
                pass
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
        
        # Обновляем сообщение с описанием фильма - используем get_film_current_state для актуального состояния
        from moviebot.bot.handlers.series import get_film_current_state, show_film_info_with_buttons
        # Получаем актуальное состояние после изменения
        current_state = get_film_current_state(chat_id, int(kp_id), user_id)
        actual_existing = current_state['existing']
        # Если API не вернул info, используем данные из БД
        if not info or not info.get('title'):
            info = {
                'title': title,
                'year': year,
                'genres': genres,
                'description': description,
                'director': director,
                'actors': actors,
                'is_series': is_series
            }
        show_film_info_with_buttons(
            chat_id, user_id, info, link, kp_id, existing=actual_existing,
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
                "◀️ Вернуться к описанию",
                chat_id,
                message_id,
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("◀️ Вернуться к описанию", callback_data=f"back_to_film:{kp_id}")
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

        markup.add(InlineKeyboardButton("◀️ Вернуться к описанию", callback_data=f"back_to_film:{kp_id}"))

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
    # ВАЖНО: Используем локальные соединения вместо глобальных
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        with db_lock:
            cursor_local.execute('''
                UPDATE plans 
                SET streaming_done = TRUE
                WHERE id = %s AND chat_id = %s
            ''', (plan_id, chat_id))
            conn_local.commit()
    except Exception as db_e:
        logger.error(f"[STREAMING DONE] Ошибка БД: {db_e}", exc_info=True)
        try:
            conn_local.rollback()
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
        # ВАЖНО: Используем локальные соединения вместо глобальных
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        
        try:
            with db_lock:
                cursor_local.execute('UPDATE movies SET watched = 1 WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                conn_local.commit()
                logger.info(f"[MARK WATCHED KP] Фильм {film_id} добавлен в базу и отмечен как просмотренный")
        except Exception as db_e:
            logger.error(f"[MARK WATCHED KP] Ошибка БД: {db_e}", exc_info=True)
            try:
                conn_local.rollback()
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
        
        # Обновляем existing (теперь watched=1)
        existing = (film_id, info.get('title'), True)
        
        # Обновляем сообщение с описанием фильма - используем get_film_current_state для актуального состояния
        from moviebot.bot.handlers.series import get_film_current_state, show_film_info_with_buttons
        # Получаем актуальное состояние после изменения
        current_state = get_film_current_state(chat_id, int(kp_id), user_id)
        actual_existing = current_state['existing']
        show_film_info_with_buttons(
            chat_id, user_id, info, link, kp_id, existing=actual_existing,
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
        # ВАЖНО: Используем локальные соединения вместо глобальных
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        
        try:
            with db_lock:
                cursor_local.execute('''
                    SELECT id, title, watched, link, kp_id, year, genres, description, director, actors, is_series
                    FROM movies WHERE id = %s AND chat_id = %s
                ''', (film_id, chat_id))
                row = cursor_local.fetchone()
                
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
                cursor_local.execute('UPDATE movies SET watched = 0 WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                conn_local.commit()
                logger.info(f"[TOGGLE WATCHED] Фильм {film_id} - отметка просмотра снята")
        except Exception as db_e:
            logger.error(f"[TOGGLE WATCHED] Ошибка БД: {db_e}", exc_info=True)
            try:
                conn_local.rollback()
            except:
                pass
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
        
        # Обновляем сообщение с описанием фильма - используем get_film_current_state для актуального состояния
        from moviebot.bot.handlers.series import get_film_current_state, show_film_info_with_buttons
        # Получаем актуальное состояние после изменения
        current_state = get_film_current_state(chat_id, int(kp_id), user_id)
        actual_existing = current_state['existing']
        show_film_info_with_buttons(
            chat_id, user_id, info, link, kp_id, existing=actual_existing,
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
        # ВАЖНО: Используем локальные соединения вместо глобальных
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        
        try:
            with db_lock:
                cursor_local.execute('SELECT title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id_str))
                row = cursor_local.fetchone()
        except Exception as db_e:
            logger.error(f"[CONFIRM REMOVE] Ошибка БД: {db_e}", exc_info=True)
            try:
                conn_local.rollback()
            except:
                pass
            bot.edit_message_text("❌ Ошибка при получении данных из базы.", chat_id, message_id)
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

        # ВАЖНО: Используем локальные соединения вместо глобальных
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        
        try:
            with db_lock:
                cursor_local.execute("""
                    SELECT id, title 
                    FROM movies 
                    WHERE chat_id = %s AND kp_id = %s
                """, (chat_id, kp_id_str))
                film = cursor_local.fetchone()

                if not film:
                    bot.edit_message_text(
                        "Фильм уже удалён или не найден.",
                        chat_id, message_id
                    )
                    return

                film_id = film[0] if isinstance(film, tuple) else film.get('id')
                title = film[1] if isinstance(film, tuple) else film.get('title', f"ID {kp_id}")

                # Удаляем всё связанное (в т.ч. подборки — иначе тег останется в списке)
                cursor_local.execute('DELETE FROM user_tag_movies WHERE film_id = %s', (film_id,))
                cursor_local.execute('DELETE FROM ratings WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
                cursor_local.execute('DELETE FROM plans WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
                cursor_local.execute('DELETE FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                conn_local.commit()
        except Exception as db_e:
            logger.error(f"[REMOVE FILM] Ошибка БД: {db_e}", exc_info=True)
            try:
                conn_local.rollback()
            except:
                pass
            bot.edit_message_text("❌ Ошибка при удалении фильма.", chat_id, message_id)
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

        # Получаем информацию о фильме через API для показа описания
        from moviebot.api.kinopoisk_api import extract_movie_info
        # Сначала пробуем получить информацию, чтобы определить is_series
        link = f"https://www.kinopoisk.ru/film/{kp_id}/"
        info = extract_movie_info(link)
        
        if info:
            # Корректируем ссылку в зависимости от типа (сериал или фильм)
            is_series = info.get('is_series', False)
            if is_series:
                link = f"https://www.kinopoisk.ru/series/{kp_id}/"
            else:
                link = f"https://www.kinopoisk.ru/film/{kp_id}/"
            
            # Обновляем описание - теперь фильм не в базе (existing=None)
            from moviebot.bot.handlers.series import show_film_info_with_buttons
            message_thread_id = getattr(call.message, 'message_thread_id', None)
            show_film_info_with_buttons(
                chat_id=chat_id,
                user_id=user_id,
                info=info,
                link=link,
                kp_id=kp_id,
                existing=None,  # Фильм удален из базы
                message_id=message_id,
                message_thread_id=message_thread_id
            )
        else:
            # Если API не вернул данные, показываем простое сообщение
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton(
                "◀️ Вернуться к описанию",
                callback_data=f"back_to_film:{kp_id}"
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

    try:
        kp_id_str = call.data.split(":")[1]
        kp_id = int(kp_id_str)
        chat_id = call.message.chat.id
        message_id = call.message.message_id

        # ВАЖНО: Используем локальные соединения вместо глобальных
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        
        try:
            with db_lock:
                # Сначала получаем название (чтобы показать нормальное сообщение)
                cursor_local.execute("""
                    SELECT id, title 
                    FROM movies 
                    WHERE chat_id = %s AND kp_id = %s
                """, (chat_id, kp_id_str))
                row = cursor_local.fetchone()

                if not row:
                    bot.edit_message_text(
                        "Фильм уже удалён или не найден.",
                        chat_id, message_id
                    )
                    return

                film_id = row[0] if isinstance(row, tuple) else row.get('id')
                title = row[1] if isinstance(row, tuple) else row.get('title', f"ID {kp_id}")

                # Удаляем
                cursor_local.execute('DELETE FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id_str))
                conn_local.commit()
        except Exception as db_e:
            logger.error(f"[CONFIRM REMOVE] Ошибка БД: {db_e}", exc_info=True)
            try:
                conn_local.rollback()
            except:
                pass
            bot.edit_message_text("❌ Ошибка при удалении фильма.", chat_id, message_id)
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

        # Кнопка "Вернуться к описанию"
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton(
            "◀️ Вернуться к описанию",
            callback_data=f"back_to_film:{kp_id}"
        ))

        bot.edit_message_text(
            f"✅ <b>{title}</b> удалён из базы!",
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=markup,
            parse_mode='HTML'
        )

        logger.info(f"[REMOVE FROM DB] Успешно удалён: kp_id={kp_id}, title='{title}'")

    except Exception as e:
        logger.error(f"[CONFIRM REMOVE] Ошибка: {e}", exc_info=True)
        # Не используем глобальный conn, так как используем локальные соединения
        try:
            bot.edit_message_text(
                "Произошла ошибка при удалении.",
                chat_id, message_id
            )
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("back_to_film:"))
def back_to_film_description(call):
    """Кнопка «◀️ Вернуться к описанию» — всегда показывает актуальную карточку с правильными кнопками для фильма/сериала"""
    logger.info(f"[BACK TO FILM] START: data={call.data}, user={call.from_user.id}")

    chat_id = call.message.chat.id
    user_id = call.from_user.id
    message_id = call.message.message_id
    message_thread_id = getattr(call.message, 'message_thread_id', None)
    
    # Очищаем состояние планирования, если пользователь вернулся к описанию
    from moviebot.states import user_plan_state
    if user_id in user_plan_state:
        state_info = user_plan_state[user_id]
        logger.info(f"[BACK TO FILM] Очищаем состояние планирования при возврате к описанию: {state_info}")
        del user_plan_state[user_id]

    # Проверяем, не устарел ли callback query, но продолжаем выполнение даже если устарел
    callback_is_old = False
    try:
        bot.answer_callback_query(call.id, text="⏳ Загружаю...")
    except Exception as answer_error:
        error_str = str(answer_error)
        if "query is too old" in error_str or "query ID is invalid" in error_str or "timeout expired" in error_str:
            callback_is_old = True
            logger.warning(f"[BACK TO FILM] Callback query устарел, но продолжаем выполнение: {answer_error}")
        else:
            logger.error(f"[BACK TO FILM] Ошибка answer_callback_query: {answer_error}", exc_info=True)

    try:

        kp_id_str = call.data.split(":", 1)[1].strip()
        try:
            kp_id_int = int(kp_id_str)
            kp_id_db = str(kp_id_int)
        except ValueError:
            logger.warning(f"[BACK TO FILM] Некорректный kp_id: {kp_id_str}")
            bot.edit_message_text("❌ Некорректная ссылка на фильм/сериал", chat_id, message_id, message_thread_id=message_thread_id)
            return

        # КРИТИЧЕСКИ ВАЖНО: Сначала получаем is_series из БД, чтобы использовать правильную ссылку для API
        is_series = False
        link_from_db = None
        info = None
        
        # 1. Получаем is_series и link из БД ПЕРВЫМ ДЕЛОМ (используем локальные соединение и курсор)
        from moviebot.database.db_connection import get_db_connection, get_db_cursor
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        try:
            with db_lock:
                cursor_local.execute("""
                    SELECT is_series, link
                    FROM movies
                    WHERE chat_id = %s AND kp_id = %s
                """, (chat_id, kp_id_db))
                row = cursor_local.fetchone()
                if row:
                    is_series = bool(row.get('is_series') if isinstance(row, dict) else row[0])
                    link_from_db = row.get('link') if isinstance(row, dict) else (row[1] if len(row) > 1 else None)
                    logger.info(f"[BACK TO FILM] is_series из БД: {is_series}, link_from_db: {link_from_db}")
        except Exception as e:
            logger.warning(f"[BACK TO FILM] Ошибка получения is_series и link из БД: {e}", exc_info=True)
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        # 2. Формируем правильную ссылку на основе is_series из БД
        if link_from_db:
            link = link_from_db
        else:
            # Если нет в БД, используем базовую ссылку (будет использована для API)
            link = f"https://www.kinopoisk.ru/series/{kp_id_int}/" if is_series else f"https://www.kinopoisk.ru/film/{kp_id_int}/"
        
        # 3. Получаем актуальное состояние через get_film_current_state (ОДИН РАЗ!)
        logger.info(f"[BACK TO FILM] Получение актуального состояния: chat_id={chat_id}, kp_id={kp_id_int}, user_id={user_id}")
        from moviebot.bot.handlers.series import get_film_current_state, show_film_info_with_buttons
        try:
            logger.info(f"[BACK TO FILM] Вызов get_film_current_state...")
            current_state = get_film_current_state(chat_id, kp_id_int, user_id)
            logger.info(f"[BACK TO FILM] get_film_current_state завершен успешно")
            existing = current_state['existing']
            logger.info(f"[BACK TO FILM] Состояние получено: existing={existing}, plan_info={current_state.get('plan_info')}")
        except Exception as state_e:
            logger.error(f"[BACK TO FILM] ❌ Ошибка в get_film_current_state: {state_e}", exc_info=True)
            # Продолжаем с пустым existing, чтобы не прерывать выполнение
            existing = None
            current_state = {'existing': None, 'plan_info': None, 'has_tickets': False, 'is_subscribed': False}
            logger.warning(f"[BACK TO FILM] Продолжаем с пустым existing из-за ошибки")
        
        # 4. ОПТИМИЗАЦИЯ: Если фильм уже в базе, используем данные из БД вместо API
        # Это экономит 1-3 секунды на запросах к API
        if existing:
            logger.info(f"[BACK TO FILM] Фильм в базе (existing={existing}), получаем данные из БД")
            # Фильм в базе - получаем данные из БД (быстро!)
            conn_local2 = get_db_connection()
            cursor_local2 = get_db_cursor()
            try:
                with db_lock:
                    cursor_local2.execute("""
                        SELECT title, year, genres, description, director, actors, is_series, link
                        FROM movies
                        WHERE chat_id = %s AND kp_id = %s
                    """, (chat_id, kp_id_db))
                    row = cursor_local2.fetchone()
                    if row:
                        info = {}
                        if isinstance(row, dict):
                            info = {
                                'title': row.get('title'),
                                'year': row.get('year'),
                                'genres': row.get('genres'),
                                'description': row.get('description'),
                                'director': row.get('director'),
                                'actors': row.get('actors'),
                                'is_series': bool(row.get('is_series', 0))
                            }
                            if not link_from_db:
                                link_from_db = row.get('link')
                        else:
                            info = {
                                'title': row[0] if len(row) > 0 else None,
                                'year': row[1] if len(row) > 1 else None,
                                'genres': row[2] if len(row) > 2 else None,
                                'description': row[3] if len(row) > 3 else None,
                                'director': row[4] if len(row) > 4 else None,
                                'actors': row[5] if len(row) > 5 else None,
                                'is_series': bool(row[6]) if len(row) > 6 else False
                            }
                            if not link_from_db and len(row) > 7:
                                link_from_db = row[7]
                        # Используем is_series из БД
                        is_series = info['is_series']
                        if link_from_db:
                            link = link_from_db
                        logger.info(f"[BACK TO FILM] Данные получены из БД (быстро!): {info.get('title')}")
            except Exception as e:
                logger.error(f"[BACK TO FILM] Ошибка чтения БД: {e}", exc_info=True)
                info = None
            finally:
                try:
                    cursor_local2.close()
                except:
                    pass
                try:
                    conn_local2.close()
                except:
                    pass
        
        # 5. Если фильм НЕ в базе или БД не дала данных, запрашиваем API
        # ВАЖНО: Это медленная операция (1-3 секунды), но необходима для фильмов не в базе
        if not info or not info.get('title'):
            logger.info(f"[BACK TO FILM] Фильм не в базе или данных нет (info={info}), запрашиваем API")
            try:
                from moviebot.api.kinopoisk_api import extract_movie_info
                logger.info(f"[BACK TO FILM] Запрос к API для kp_id={kp_id_int}, link={link}")
                info = extract_movie_info(link)
                if info and info.get('title'):
                    logger.info(f"[BACK TO FILM] API успех: {info['title']}")
                    # ВАЖНО: Если is_series уже получен из БД, используем его, а не из API
                    if not link_from_db:  # Только если фильм не в БД, используем is_series из API
                        is_series = info.get('is_series', False)
                        # Обновляем link на основе is_series из API
                        if is_series:
                            link = f"https://www.kinopoisk.ru/series/{kp_id_int}/"
                        else:
                            link = f"https://www.kinopoisk.ru/film/{kp_id_int}/"
                        logger.info(f"[BACK TO FILM] Обновлен link на основе API: {link}")
                else:
                    logger.warning(f"[BACK TO FILM] API вернул пустой результат: info={info}")
            except Exception as e:
                logger.error(f"[BACK TO FILM] API не сработал: {e}", exc_info=True)
        
        # 6. Если фильм в базе, но API не дал данных, получаем из БД (fallback)
        if existing and (not info or not info.get('title')):
            conn_local3 = get_db_connection()
            cursor_local3 = get_db_cursor()
            try:
                with db_lock:
                    cursor_local3.execute("""
                        SELECT title, year, genres, description, director, actors, is_series, link
                        FROM movies
                        WHERE chat_id = %s AND kp_id = %s
                    """, (chat_id, kp_id_db))
                    row = cursor_local3.fetchone()
                    if row:
                        info = info or {}
                        if isinstance(row, dict):
                            info.update({
                                'title': row.get('title'),
                                'year': row.get('year'),
                                'genres': row.get('genres'),
                                'description': row.get('description'),
                                'director': row.get('director'),
                                'actors': row.get('actors'),
                                'is_series': bool(row.get('is_series', 0))
                            })
                            if not link_from_db:
                                link_from_db = row.get('link')
                        else:
                            info.update({
                                'title': row[0] if len(row) > 0 else None,
                                'year': row[1] if len(row) > 1 else None,
                                'genres': row[2] if len(row) > 2 else None,
                                'description': row[3] if len(row) > 3 else None,
                                'director': row[4] if len(row) > 4 else None,
                                'actors': row[5] if len(row) > 5 else None,
                                'is_series': bool(row[6]) if len(row) > 6 else False
                            })
                            if not link_from_db and len(row) > 7:
                                link_from_db = row[7]
                        # Используем is_series из БД
                        is_series = info['is_series']
                        if link_from_db:
                            link = link_from_db
            except Exception as e:
                logger.error(f"[BACK TO FILM] Ошибка чтения БД: {e}", exc_info=True)
            finally:
                try:
                    cursor_local3.close()
                except:
                    pass
                try:
                    conn_local3.close()
                except:
                    pass

        if not info or not info.get('title'):
            logger.error(f"[BACK TO FILM] ❌ Нет данных для показа: info={info}, existing={existing}, kp_id={kp_id_int}")
            try:
                if message_id and not callback_is_old:
                    bot.edit_message_text(
                        "❌ Не удалось загрузить информацию о фильме/сериале",
                        chat_id, message_id, message_thread_id=message_thread_id
                    )
                else:
                    bot.send_message(
                        chat_id,
                        "❌ Не удалось загрузить информацию о фильме/сериале",
                        message_thread_id=message_thread_id
                    )
            except Exception as send_e:
                logger.error(f"[BACK TO FILM] Ошибка отправки сообщения об ошибке: {send_e}", exc_info=True)
            return
        
        logger.info(f"[BACK TO FILM] ✅ Данные получены: title={info.get('title')}, is_series={is_series}, link={link}")

        # Убеждаемся, что is_series правильно установлен в info (приоритет у БД)
        info['is_series'] = is_series
        
        # Уточняем link для сериала/фильма (используем ссылку из БД если есть, иначе формируем на основе is_series)
        if not link_from_db:
            if is_series:
                link = f"https://www.kinopoisk.ru/series/{kp_id_int}/"
            else:
                link = f"https://www.kinopoisk.ru/film/{kp_id_int}/"
        # Если link_from_db есть, он уже установлен выше
        
        logger.info(f"[BACK TO FILM] Подготовка к вызову show_film_info_with_buttons: link={link}, is_series={is_series}, title={info.get('title')}")

        # ОПТИМИЗАЦИЯ: Всегда отправляем новое сообщение, не редактируем старое
        # Это работает быстрее и не требует редактирования сообщения с кнопкой
        # Пользователь просто получает новое сообщение с описанием, как при отправке ссылки
        logger.info(f"[BACK TO FILM] Отправляем новое сообщение с описанием (оптимизировано)")
        
        # Главный вызов — передаем message_id=None чтобы всегда отправлялось новое сообщение
        # Работает как отправка ссылки в чат - просто показывает описание фильма
        try:
            logger.info(f"[BACK TO FILM] ===== ВЫЗОВ show_film_info_with_buttons =====")
            logger.info(f"[BACK TO FILM] Параметры: chat_id={chat_id}, user_id={user_id}, kp_id={kp_id_int}")
            logger.info(f"[BACK TO FILM] Параметры: title={info.get('title')}, is_series={is_series}, existing={existing}")
            logger.info(f"[BACK TO FILM] Параметры: message_id=None (новое сообщение), message_thread_id={message_thread_id}")
            
            show_film_info_with_buttons(
                chat_id=chat_id,
                user_id=user_id,
                info=info,
                link=link,
                kp_id=kp_id_int,
                existing=existing,  # Может быть None, тогда внутри функции будет получен актуальный
                message_id=None,  # Всегда новое сообщение (оптимизация)
                message_thread_id=message_thread_id
            )
            logger.info(f"[BACK TO FILM] ✅ show_film_info_with_buttons завершена успешно")
        except Exception as show_e:
            logger.error(f"[BACK TO FILM] ❌ ОШИБКА в show_film_info_with_buttons: {show_e}", exc_info=True)
            # Пытаемся отправить сообщение об ошибке
            try:
                if message_id and not callback_is_old:
                    bot.edit_message_text(
                        f"❌ Ошибка при загрузке описания: {str(show_e)[:100]}",
                        chat_id, message_id, message_thread_id=message_thread_id
                    )
                else:
                    bot.send_message(chat_id, f"❌ Ошибка при загрузке описания: {str(show_e)[:100]}", message_thread_id=message_thread_id)
            except:
                pass

        logger.info(f"[BACK TO FILM] ===== КОНЕЦ ОБРАБОТКИ ===== is_series={is_series}, existing={'есть' if existing else 'нет'}")

    except Exception as e:
        logger.error(f"[BACK TO FILM] ❌ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
        try:
            from moviebot.database.db_connection import get_db_connection
            conn_local_error = get_db_connection()
            try:
                conn_local_error.rollback()
            except:
                pass
            if message_id:
                try:
                    bot.edit_message_text("❌ Ошибка при загрузке описания", chat_id, message_id, message_thread_id=message_thread_id)
                except:
                    bot.send_message(chat_id, "❌ Ошибка при загрузке описания", message_thread_id=message_thread_id)
            else:
                bot.send_message(chat_id, "❌ Ошибка при загрузке описания", message_thread_id=message_thread_id)
        except Exception as final_err:
            logger.error(f"[BACK TO FILM] Ошибка в блоке обработки ошибок: {final_err}", exc_info=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("show_film:"))
def show_film_callback(call):
    """Обработчик кнопки «📖 К описанию» — показывает описание фильма (аналог back_to_film_description)"""
    logger.info(f"[SHOW FILM] START: data={call.data}, user={call.from_user.id}")
    
    # Используем ту же логику, что и back_to_film_description
    chat_id = call.message.chat.id
    user_id = call.from_user.id
    message_id = call.message.message_id
    message_thread_id = getattr(call.message, 'message_thread_id', None)
    
    # Проверяем, не устарел ли callback query
    callback_is_old = False
    try:
        bot.answer_callback_query(call.id, text="⏳ Загружаю...")
    except Exception as answer_error:
        error_str = str(answer_error)
        if "query is too old" in error_str or "query ID is invalid" in error_str or "timeout expired" in error_str:
            callback_is_old = True
            logger.warning(f"[SHOW FILM] Callback query устарел: {answer_error}")
        else:
            logger.error(f"[SHOW FILM] Ошибка answer_callback_query: {answer_error}", exc_info=True)
    
    try:
        kp_id_str = call.data.split(":", 1)[1].strip()
        kp_id_int = int(kp_id_str)
        
        # Получаем данные о фильме (та же логика что и в back_to_film_description)
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        
        existing = None
        link_from_db = None
        is_series = False
        
        with db_lock:
            try:
                cursor_local.execute("""
                    SELECT id, title, watched, link, is_series
                    FROM movies 
                    WHERE chat_id = %s AND kp_id = %s
                """, (chat_id, kp_id_str))
                row = cursor_local.fetchone()
                
                if row:
                    film_id = row.get('id') if isinstance(row, dict) else row[0]
                    title_db = row.get('title') if isinstance(row, dict) else row[1]
                    watched = row.get('watched') if isinstance(row, dict) else row[2]
                    link_from_db = row.get('link') if isinstance(row, dict) else row[3]
                    is_series_db = row.get('is_series') if isinstance(row, dict) else row[4]
                    is_series = bool(is_series_db) if is_series_db is not None else False
                    existing = (film_id, title_db, watched)
                    
                conn_local.commit()
            except Exception as db_err:
                logger.error(f"[SHOW FILM] Ошибка БД: {db_err}", exc_info=True)
                try:
                    conn_local.rollback()
                except:
                    pass
        
        # Получаем информацию о фильме через API
        from moviebot.api.kinopoisk_api import extract_movie_info
        
        link = link_from_db if link_from_db else (f"https://www.kinopoisk.ru/series/{kp_id_int}/" if is_series else f"https://www.kinopoisk.ru/film/{kp_id_int}/")
        info = extract_movie_info(link)
        
        if not info:
            bot.answer_callback_query(call.id, "❌ Не удалось загрузить информацию о фильме", show_alert=True)
            return
        
        info['is_series'] = is_series
        
        # Показываем описание фильма
        from moviebot.bot.handlers.series import show_film_info_with_buttons
        show_film_info_with_buttons(
            chat_id=chat_id,
            user_id=user_id,
            info=info,
            link=link,
            kp_id=kp_id_int,
            existing=existing,
            message_id=None,  # Всегда новое сообщение
            message_thread_id=message_thread_id
        )
        
        bot.answer_callback_query(call.id, "✅ Готово!")
        
    except Exception as e:
        logger.error(f"[SHOW FILM] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка загрузки", show_alert=True)
        except:
            pass

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


