"""
Callback handlers для карточки фильма (add_to_database, plan_from_added, show_facts и т.д.)
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.bot.bot_init import bot as bot_instance
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.api.kinopoisk_api import get_facts
from moviebot.states import user_plan_state

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


@bot_instance.callback_query_handler(func=lambda call: call.data.startswith("add_to_database:"))
def add_to_database_callback(call):
    """Обработчик кнопки '➕ Добавить в базу'"""
    logger.info("=" * 80)
    logger.info(f"[ADD TO DATABASE] ===== START: callback_id={call.id}, callback_data={call.data}")
    try:
        bot_instance.answer_callback_query(call.id, text="⏳ Добавляю в базу...")
        logger.info(f"[ADD TO DATABASE] answer_callback_query вызван, callback_id={call.id}")
        
        kp_id = call.data.split(":")[1]
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        logger.info(f"[ADD TO DATABASE] Пользователь {user_id} хочет добавить фильм kp_id={kp_id} в базу, chat_id={chat_id}")
        
        # Проверяем, есть ли фильм уже в базе
        with db_lock:
            cursor.execute('SELECT id, title, link, watched, is_series FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
        
        if row:
            # Фильм уже в базе
            film_id = row.get('id') if isinstance(row, dict) else row[0]
            title_db = row.get('title') if isinstance(row, dict) else row[1]
            link = row.get('link') if isinstance(row, dict) else row[2]
            watched = row.get('watched') if isinstance(row, dict) else row[3]
            
            logger.info(f"[ADD TO DATABASE] Фильм уже в базе: film_id={film_id}, title={title_db}")
            bot_instance.answer_callback_query(call.id, f"ℹ️ {title_db} уже в базе", show_alert=False)
            
            # Обновляем сообщение, показывая что фильм в базе
            from moviebot.bot.handlers.series import show_film_info_with_buttons
            # Получаем минимальную информацию из базы для обновления карточки
            # Не делаем запрос к API - используем данные из базы
            info = {
                'title': title_db,
                'year': None,  # Можно получить из базы, но не обязательно
                'is_series': bool(row.get('is_series') if isinstance(row, dict) else row[4]) if len(row) > 4 else False
            }
            show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=(film_id, title_db, watched), message_id=call.message.message_id)
            return
        
        # Фильма нет в базе - добавляем с минимальной информацией из текста сообщения
        # НЕ ДЕЛАЕМ ЗАПРОС К API - используем информацию из сообщения
        message_text = call.message.text or ""
        logger.info(f"[ADD TO DATABASE] Фильм не найден в базе, извлекаю информацию из сообщения")
        
        # Извлекаем название из текста сообщения (обычно первая строка после эмодзи)
        import re
        title_match = re.search(r'[📺🎬]\s*<b>(.*?)</b>', message_text)
        if title_match:
            title = title_match.group(1)
        else:
            # Пробуем без HTML тегов
            title_match = re.search(r'[📺🎬]\s*(.+?)\s*\(', message_text)
            if title_match:
                title = title_match.group(1).strip()
            else:
                # Если не нашли - используем kp_id как заглушку
                title = f"Фильм {kp_id}"
        
        # Определяем, фильм это или сериал по эмодзи в сообщении
        is_series = '📺' in message_text
        link = f"https://www.kinopoisk.ru/series/{kp_id}/" if is_series else f"https://www.kinopoisk.ru/film/{kp_id}/"
        
        logger.info(f"[ADD TO DATABASE] Добавляю фильм в базу: title={title}, is_series={is_series}, link={link}")
        
        # Добавляем фильм в базу с минимальной информацией
        with db_lock:
            cursor.execute('''
                INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series, added_by, added_at, source)
                VALUES (%s, %s, %s, %s, NULL, NULL, NULL, NULL, NULL, %s, %s, NOW(), 'button')
                ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link
                RETURNING id, title, watched
            ''', (chat_id, link, kp_id, title, 1 if is_series else 0, user_id))
            
            result = cursor.fetchone()
            film_id = result.get('id') if isinstance(result, dict) else result[0]
            title_db = result.get('title') if isinstance(result, dict) else result[1]
            watched = result.get('watched') if isinstance(result, dict) else result[2]
            conn.commit()
        
        logger.info(f"[ADD TO DATABASE] Фильм добавлен в базу: film_id={film_id}, title={title_db}")
        bot_instance.answer_callback_query(call.id, f"✅ {title_db} добавлен в базу!", show_alert=False)
        
        # Обновляем сообщение, показывая что фильм теперь в базе
        from moviebot.bot.handlers.series import show_film_info_with_buttons
        info = {
            'title': title_db,
            'year': None,
            'is_series': is_series
        }
        show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=(film_id, title_db, watched), message_id=call.message.message_id)
        
    except Exception as e:
        logger.error(f"[ADD TO DATABASE] КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except Exception as answer_e:
            logger.error(f"[ADD TO DATABASE] Не удалось вызвать answer_callback_query: {answer_e}")
    finally:
        logger.info(f"[ADD TO DATABASE] ===== END: callback_id={call.id}")


@bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("plan_from_added:"))
def plan_from_added_callback(call):
    """Обработчик планирования из добавленного фильма"""
    logger.info(f"[PLAN FROM ADDED] ===== НАЧАЛО ОБРАБОТКИ =====")
    logger.info(f"[PLAN FROM ADDED] Получен callback: call.data={call.data}, user_id={call.from_user.id}, chat_id={call.message.chat.id}")
    try:
        bot_instance.answer_callback_query(call.id)  # Отвечаем сразу, чтобы убрать "крутилку"
        
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        kp_id = call.data.split(":")[1]
        
        logger.info(f"[PLAN FROM ADDED] Пользователь {user_id} хочет запланировать фильм kp_id={kp_id}")
        
        # Проверяем, есть ли фильм в базе, если нет - добавляем с минимальной информацией
        
        link = None
        film_id = None
        with db_lock:
            cursor.execute('SELECT id, link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
            if row:
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                link = row.get('link') if isinstance(row, dict) else row[1]
                logger.info(f"[PLAN FROM ADDED] Фильм найден в базе: film_id={film_id}, link={link}")
        
        if not film_id:
            # Фильм не в базе - добавляем с минимальной информацией из сообщения
            # НЕ ДЕЛАЕМ ЗАПРОС К API - используем информацию из сообщения
            message_text = call.message.text or ""
            import re
            title_match = re.search(r'[📺🎬]\s*<b>(.*?)</b>', message_text)
            if title_match:
                title = title_match.group(1)
            else:
                title_match = re.search(r'[📺🎬]\s*(.+?)\s*\(', message_text)
                if title_match:
                    title = title_match.group(1).strip()
                else:
                    title = f"Фильм {kp_id}"
            
            is_series = '📺' in message_text
            if not link:
                link = f"https://kinopoisk.ru/series/{kp_id}/" if is_series else f"https://kinopoisk.ru/film/{kp_id}/"
            
            logger.info(f"[PLAN FROM ADDED] Добавляю фильм в базу при планировании: title={title}, kp_id={kp_id}")
            
            # Добавляем фильм в базу с минимальной информацией
            with db_lock:
                cursor.execute('''
                    INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series, added_by, added_at, source)
                    VALUES (%s, %s, %s, %s, NULL, NULL, NULL, NULL, NULL, %s, %s, NOW(), 'plan_button')
                    ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link
                    RETURNING id
                ''', (chat_id, link, kp_id, title, 1 if is_series else 0, user_id))
                
                result = cursor.fetchone()
                film_id = result.get('id') if isinstance(result, dict) else result[0]
                conn.commit()
            
            if not film_id:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка при добавлении фильма в базу", show_alert=True)
                return
            
            logger.info(f"[PLAN FROM ADDED] Фильм добавлен в базу при планировании: kp_id={kp_id}, film_id={film_id}")
        
        if not link:
            link = f"https://kinopoisk.ru/film/{kp_id}/"
            logger.info(f"[PLAN FROM ADDED] Ссылка не найдена в базе, используем стандартную: {link}")
        
        user_plan_state[user_id] = {
            'step': 2,
            'link': link,
            'chat_id': chat_id
        }
        
        logger.info(f"[PLAN FROM ADDED] Состояние установлено: user_id={user_id}, state={user_plan_state[user_id]}")
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("Дома", callback_data="plan_type:home"))
        markup.add(InlineKeyboardButton("В кино", callback_data="plan_type:cinema"))
        
        logger.info(f"[PLAN FROM ADDED] Отправка сообщения с выбором типа просмотра...")
        bot_instance.send_message(chat_id, "Где планируете смотреть?", reply_markup=markup)
        logger.info(f"[PLAN FROM ADDED] Сообщение отправлено успешно")
    except Exception as e:
        logger.error(f"[PLAN FROM ADDED] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass
    finally:
        logger.info(f"[PLAN FROM ADDED] ===== КОНЕЦ ОБРАБОТКИ =====")


@bot_instance.callback_query_handler(func=lambda call: call.data.startswith("show_facts:") or call.data.startswith("facts:"))
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
            bot_instance.send_message(chat_id, facts, parse_mode='HTML')
            bot_instance.answer_callback_query(call.id, "Факты отправлены")
        else:
            bot_instance.answer_callback_query(call.id, "Факты не найдены", show_alert=True)
    except Exception as e:
        logger.error(f"[SHOW FACTS] Ошибка: {e}", exc_info=True)
    finally:
        # ВСЕГДА отвечаем на callback!
        try:
            bot_instance.answer_callback_query(call.id)
        except Exception as answer_e:
            logger.error(f"[SHOW FACTS] Не удалось ответить на callback: {answer_e}", exc_info=True)


def register_film_callbacks(bot_instance):
    """Регистрирует callback handlers для карточки фильма (уже зарегистрированы через декораторы)"""
    # Handlers уже зарегистрированы через декораторы @bot_instance.callback_query_handler
    # при импорте модуля, поэтому эта функция просто для совместимости
    pass

