"""
Обработчики для работы с тегами/подборками фильмов
"""
import logging
import re
import secrets
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from moviebot.bot.bot_init import bot
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.utils.admin import is_admin, is_owner
from moviebot.api.kinopoisk_api import extract_movie_info
from moviebot.utils.parsing import extract_kp_id_from_text
from moviebot.bot.handlers.series import ensure_movie_in_database
from moviebot.states import user_plan_state, user_view_film_state, user_mark_watched_state

logger = logging.getLogger(__name__)
logger.info("=" * 80)
logger.info("[TAGS] Модуль tags.py импортирован - декораторы будут зарегистрированы")
logger.info("=" * 80)

# Состояние для обработки команды /add_tags
user_add_tag_state = {}


@bot.message_handler(commands=['add_tags'])
def add_tags_command(message):
    """Команда /add_tags - создание подборки фильмов (только для админов)"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Проверяем права администратора
    if not (is_admin(user_id) or is_owner(user_id)):
        bot.reply_to(message, "❌ Эта команда доступна только администраторам.")
        return
    
    # Сначала отправляем промпт
    prompt_msg = bot.reply_to(
        message,
        "📝 <b>Создание подборки</b>\n\n"
        "В ответном сообщении пришлите:\n"
        "• Название подборки в кавычках (например: \"watch с Викулей\")\n"
        "• Ссылки на фильмы/сериалы с Кинопоиска\n\n"
        "Пример:\n"
        "<code>\"watch с Викулей\"\n"
        "https://www.kinopoisk.ru/film/123/\n"
        "https://www.kinopoisk.ru/series/456/</code>",
        parse_mode='HTML'
    )
    
    # Сохраняем message_id промпта ПЕРЕД установкой состояния
    prompt_message_id = prompt_msg.message_id if prompt_msg else None
    logger.info(f"[ADD TAG COMMAND] Создан промпт с message_id={prompt_message_id} для user_id={user_id}")
    
    # Устанавливаем состояние с правильным prompt_message_id
    user_add_tag_state[user_id] = {
        'step': 'waiting_for_tag_data',
        'chat_id': chat_id,
        'prompt_message_id': prompt_message_id
    }
    
    logger.info(f"[ADD TAG COMMAND] Состояние установлено: {user_add_tag_state[user_id]}")


def check_add_tag_reply(message):
    """Проверяет, является ли сообщение ответом для команды /add_tags - ТОЛЬКО РЕПЛАИ НА ПРОМПТ"""
    # КРИТИЧЕСКОЕ ЛОГИРОВАНИЕ - проверяем, вызывается ли функция вообще
    import sys
    print(f"[CHECK ADD TAG REPLY] ===== ВЫЗВАНА: user_id={message.from_user.id}, message_id={message.message_id}", file=sys.stdout, flush=True)
    
    user_id = message.from_user.id
    logger.info(f"[CHECK ADD TAG REPLY] ===== START: user_id={user_id}, message_id={message.message_id}, has_reply={message.reply_to_message is not None}, text_preview='{message.text[:50] if message.text else None}'")
    
    if user_id not in user_add_tag_state:
        logger.info(f"[CHECK ADD TAG REPLY] ❌ user_id={user_id} НЕ в user_add_tag_state")
        return False
    
    state = user_add_tag_state[user_id]
    logger.info(f"[CHECK ADD TAG REPLY] Состояние найдено: {state}")
    
    if state.get('step') != 'waiting_for_tag_data':
        logger.info(f"[CHECK ADD TAG REPLY] ❌ step={state.get('step')} != 'waiting_for_tag_data'")
        return False
    
    # СТРОГАЯ ПРОВЕРКА: ТОЛЬКО реплаи на промпт
    if not message.reply_to_message:
        logger.info(f"[CHECK ADD TAG REPLY] ❌ Сообщение НЕ является реплаем, пропускаем для user_id={user_id}")
        return False
    
    prompt_message_id = state.get('prompt_message_id')
    if not prompt_message_id:
        logger.info(f"[CHECK ADD TAG REPLY] ❌ prompt_message_id не найден в состоянии для user_id={user_id}")
        return False
    
    reply_to_id = message.reply_to_message.message_id
    if reply_to_id != prompt_message_id:
        logger.info(f"[CHECK ADD TAG REPLY] ❌ Сообщение является реплаем, но НЕ на промпт /add_tags (reply_to={reply_to_id}, expected={prompt_message_id}) для user_id={user_id}")
        return False
    
    logger.info(f"[CHECK ADD TAG REPLY] ✅ Сообщение является ответом на промпт /add_tags для user_id={user_id}, message_id={message.message_id}")
    return True


# Регистрируем обработчик с функцией проверки - это гарантирует правильную работу
@bot.message_handler(content_types=['text'], func=check_add_tag_reply)
def handle_add_tag_reply(message):
    """Обработчик ответа на команду /add_tags - срабатывает ТОЛЬКО для админов в состоянии /add_tags"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text or ""
    
    logger.info(f"[ADD TAG] ===== START: Обработка сообщения от user_id={user_id}, text_length={len(text)}, message_id={message.message_id}")
    logger.info(f"[ADD TAG] ✅ ОБРАБОТЧИК СРАБОТАЛ! check_add_tag_reply вернул True")
    
    try:
        # Извлекаем название тега из кавычек
        tag_name_match = re.search(r'["""]([^"""]+)["""]', text)
        if not tag_name_match:
            bot.reply_to(message, "❌ Не найдено название подборки в кавычках. Пример: \"watch с Викулей\"")
            if user_id in user_add_tag_state:
                del user_add_tag_state[user_id]
            return
        
        tag_name = tag_name_match.group(1).strip()
        if not tag_name:
            bot.reply_to(message, "❌ Название подборки не может быть пустым.")
            if user_id in user_add_tag_state:
                del user_add_tag_state[user_id]
            return
        
        # Извлекаем все kp_id из текста
        kp_ids = set()
        
        # 1. Ищем ссылки на Кинопоиск (полные URL)
        links = re.findall(r'https?://(?:www\.)?kinopoisk\.(?:ru|com)/(?:film|series)/(\d+)', text, re.IGNORECASE)
        for link_match in links:
            kp_ids.add(link_match)
            logger.info(f"[ADD TAG] Найдена ссылка: {link_match}")
        
        # 2. Ищем короткие ссылки типа kinopoisk.ru/film/123 (без протокола)
        short_links = re.findall(r'kinopoisk\.(?:ru|com)/(?:film|series)/(\d+)', text, re.IGNORECASE)
        for short_link in short_links:
            kp_ids.add(short_link)
            logger.info(f"[ADD TAG] Найдена короткая ссылка: {short_link}")
        
        # 3. Ищем ID через запятую или пробел (например: "10246904, 5268266, 8106285" или "10246904 5268266 8106285")
        # Ищем последовательности цифр любой длины (kp_id может быть коротким, например 474, 488)
        # НО: исключаем те, что уже найдены в ссылках
        # Сначала ищем длинные ID (4+ цифр) - они точно ID
        id_pattern_long = r'\b\d{4,10}\b'
        found_ids_long = re.findall(id_pattern_long, text)
        for found_id in found_ids_long:
            # Проверяем, что это не часть ссылки (уже обработано выше)
            found_pos = text.find(found_id)
            if found_pos > 0:
                before = text[max(0, found_pos-20):found_pos].lower()
                after = text[found_pos+len(found_id):min(len(text), found_pos+len(found_id)+5)]
                # Если это часть ссылки, пропускаем
                if 'kinopoisk' in before or '/' in after:
                    continue
            kp_ids.add(found_id)
            logger.info(f"[ADD TAG] Найден ID: {found_id}")
        
        # Теперь ищем короткие ID (1-3 цифры) - только если они стоят отдельно (окружены пробелами/запятыми)
        # Это нужно для случаев типа "474, 488" где ID короткие
        # Находим позицию конца названия в кавычках
        quote_end_pos = text.rfind('"')
        if quote_end_pos >= 0:
            # Ищем ID только после закрывающей кавычки
            text_after_quote = text[quote_end_pos + 1:].strip()
            if text_after_quote:
                # Разбиваем текст после кавычек по запятым и пробелам
                # Берем все части, которые являются числами длиной 1-3 цифры
                parts = re.split(r'[\s,]+', text_after_quote)
                for part in parts:
                    part = part.strip()
                    if part and part.isdigit() and 1 <= len(part) <= 3:
                        # Проверяем, что это не часть ссылки
                        found_pos_in_full = text.find(part, quote_end_pos)
                        if found_pos_in_full > 0:
                            before = text[max(0, found_pos_in_full-20):found_pos_in_full].lower()
                            after = text[found_pos_in_full+len(part):min(len(text), found_pos_in_full+len(part)+5)]
                            # Если это часть ссылки, пропускаем
                            if 'kinopoisk' in before or '/' in after:
                                continue
                        kp_ids.add(part)
                        logger.info(f"[ADD TAG] Найден короткий ID: {part}")
        
        logger.info(f"[ADD TAG] Всего найдено уникальных kp_id: {len(kp_ids)}")
        
        if not kp_ids:
            bot.reply_to(message, "❌ Не найдено ссылок или ID фильмов/сериалов с Кинопоиска.\n\nМожно указать:\n• Ссылки: https://www.kinopoisk.ru/film/123/\n• ID через запятую: 10246904, 5268266, 8106285")
            if user_id in user_add_tag_state:
                del user_add_tag_state[user_id]
            return
        
        logger.info(f"[ADD TAG] Найдено {len(kp_ids)} уникальных kp_id для подборки '{tag_name}'")
        
        # Проверяем, существует ли уже подборка с таким названием, созданная тем же пользователем
        conn_check = get_db_connection()
        cursor_check = get_db_cursor()
        existing_tag_id = None
        existing_tag_code = None
        existing_tag_created_by = None
        
        try:
            with db_lock:
                cursor_check.execute('SELECT id, short_code, created_by FROM tags WHERE name = %s', (tag_name,))
                row = cursor_check.fetchone()
                if row:
                    existing_tag_id = row.get('id') if isinstance(row, dict) else row[0]
                    existing_tag_code = row.get('short_code') if isinstance(row, dict) else row[1]
                    existing_tag_created_by = row.get('created_by') if isinstance(row, dict) else row[2]
                    logger.info(f"[ADD TAG] Найдена существующая подборка с таким названием: id={existing_tag_id}, code={existing_tag_code}, created_by={existing_tag_created_by}")
        except Exception as e:
            logger.error(f"[ADD TAG] Ошибка проверки существующей подборки: {e}", exc_info=True)
        finally:
            try:
                cursor_check.close()
            except:
                pass
            try:
                conn_check.close()
            except:
                pass
        
        # Если подборка существует и создана тем же пользователем - предлагаем добавить фильмы
        if existing_tag_id and existing_tag_created_by == user_id:
            # Проверяем, сколько новых фильмов будет добавлено (исключая дубли)
            conn_count = get_db_connection()
            cursor_count = get_db_cursor()
            new_films_count = 0
            try:
                with db_lock:
                    for kp_id in kp_ids:
                        cursor_count.execute('SELECT id FROM tag_movies WHERE tag_id = %s AND kp_id = %s', (existing_tag_id, kp_id))
                        if not cursor_count.fetchone():
                            new_films_count += 1
            except Exception as e:
                logger.error(f"[ADD TAG] Ошибка подсчета новых фильмов: {e}", exc_info=True)
            finally:
                try:
                    cursor_count.close()
                except:
                    pass
                try:
                    conn_count.close()
                except:
                    pass
            
            if new_films_count == 0:
                bot.reply_to(message, f"ℹ️ Все указанные фильмы/сериалы уже есть в подборке <b>\"{tag_name}\"</b>.", parse_mode='HTML')
                if user_id in user_add_tag_state:
                    del user_add_tag_state[user_id]
                return
            
            # Предлагаем добавить фильмы к существующему тегу
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("✅ Добавить к существующей подборке", callback_data=f"tag_add_to_existing:{existing_tag_id}:{tag_name}"))
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="tag_cancel_add"))
            
            bot.reply_to(
                message,
                f"📦 Подборка с названием <b>\"{tag_name}\"</b> уже существует.\n\n"
                f"Будет добавлено <b>{new_films_count}</b> новых фильмов/сериалов (дубли будут пропущены).\n\n"
                f"Добавить фильмы к существующей подборке?",
                parse_mode='HTML',
                reply_markup=markup
            )
            
            # Сохраняем данные для обработки подтверждения
            if user_id not in user_add_tag_state:
                user_add_tag_state[user_id] = {}
            user_add_tag_state[user_id]['pending_add'] = {
                'tag_id': existing_tag_id,
                'tag_name': tag_name,
                'kp_ids': list(kp_ids),
                'short_code': existing_tag_code
            }
            return
        else:
            # Генерируем короткий код для ссылки
            short_code = secrets.token_urlsafe(8).upper()[:12]  # 12 символов
            
            # Проверяем уникальность кода
            conn_code = get_db_connection()
            cursor_code = get_db_cursor()
            code_unique = False
            attempts = 0
            while not code_unique and attempts < 10:
                try:
                    with db_lock:
                        cursor_code.execute('SELECT id FROM tags WHERE short_code = %s', (short_code,))
                        if not cursor_code.fetchone():
                            code_unique = True
                        else:
                            short_code = secrets.token_urlsafe(8).upper()[:12]
                            attempts += 1
                except:
                    pass
            
            try:
                cursor_code.close()
            except:
                pass
            try:
                conn_code.close()
            except:
                pass
            
            if not code_unique:
                bot.reply_to(message, "❌ Не удалось сгенерировать уникальный код. Попробуйте позже.")
                if user_id in user_add_tag_state:
                    del user_add_tag_state[user_id]
                return
            
            # Создаем тег в БД
            conn = get_db_connection()
            cursor = get_db_cursor()
            
            try:
                with db_lock:
                    cursor.execute('''
                        INSERT INTO tags (name, short_code, created_by)
                        VALUES (%s, %s, %s)
                        RETURNING id
                    ''', (tag_name, short_code, user_id))
                    row = cursor.fetchone()
                    tag_id = row.get('id') if isinstance(row, dict) else row[0]
                    conn.commit()
                    logger.info(f"[ADD TAG] Создан тег id={tag_id}, name='{tag_name}', code={short_code}")
            except Exception as e:
                logger.error(f"[ADD TAG] Ошибка создания тега: {e}", exc_info=True)
                try:
                    conn.rollback()
                except:
                    pass
                bot.reply_to(message, "❌ Ошибка при создании подборки в базе данных.")
                if user_id in user_add_tag_state:
                    del user_add_tag_state[user_id]
                return
            finally:
                try:
                    cursor.close()
                except:
                    pass
                try:
                    conn.close()
                except:
                    pass
        
        # Добавляем фильмы в тег (только новые, если подборка уже существовала)
        added_count = 0
        already_in_tag = 0
        errors = []
        
        for kp_id in kp_ids:
            try:
                # Определяем, фильм это или сериал
                link = f"https://www.kinopoisk.ru/film/{kp_id}/"
                info = extract_movie_info(link)
                
                if not info:
                    # Пробуем как сериал
                    link = f"https://www.kinopoisk.ru/series/{kp_id}/"
                    info = extract_movie_info(link)
                
                if not info:
                    errors.append(f"{kp_id}: не удалось получить информацию")
                    continue
                
                is_series = info.get('is_series', False)
                
                # Добавляем фильм в админскую базу для быстрого получения названий
                ADMIN_CHAT_ID = 301810276
                conn_admin = get_db_connection()
                cursor_admin = get_db_cursor()
                try:
                    with db_lock:
                        # Проверяем, есть ли уже в админской базе
                        cursor_admin.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (ADMIN_CHAT_ID, kp_id))
                        if not cursor_admin.fetchone():
                            # Добавляем в админскую базу
                            from moviebot.bot.handlers.series import ensure_movie_in_database
                            ensure_movie_in_database(ADMIN_CHAT_ID, kp_id, link, info, ADMIN_CHAT_ID)
                            logger.info(f"[ADD TAG] Добавлен kp_id={kp_id} в админскую базу для быстрого доступа")
                except Exception as e:
                    logger.warning(f"[ADD TAG] Ошибка добавления в админскую базу kp_id={kp_id}: {e}")
                finally:
                    try:
                        cursor_admin.close()
                    except:
                        pass
                    try:
                        conn_admin.close()
                    except:
                        pass
                
                # Добавляем в tag_movies (проверяем, не добавлен ли уже)
                conn_add = get_db_connection()
                cursor_add = get_db_cursor()
                try:
                    with db_lock:
                        # Проверяем, есть ли уже этот фильм в подборке
                        cursor_add.execute('SELECT id FROM tag_movies WHERE tag_id = %s AND kp_id = %s', (tag_id, kp_id))
                        if cursor_add.fetchone():
                            already_in_tag += 1
                            logger.info(f"[ADD TAG] kp_id={kp_id} уже есть в подборке {tag_id}, пропускаем")
                        else:
                            cursor_add.execute('''
                                INSERT INTO tag_movies (tag_id, kp_id, is_series)
                                VALUES (%s, %s, %s)
                            ''', (tag_id, kp_id, is_series))
                            conn_add.commit()
                            added_count += 1
                            logger.info(f"[ADD TAG] Добавлен kp_id={kp_id} (is_series={is_series}) в тег {tag_id}")
                except Exception as e:
                    logger.error(f"[ADD TAG] Ошибка добавления kp_id={kp_id}: {e}")
                    errors.append(f"{kp_id}: ошибка БД")
                finally:
                    try:
                        cursor_add.close()
                    except:
                        pass
                    try:
                        conn_add.close()
                    except:
                        pass
                        
            except Exception as e:
                logger.error(f"[ADD TAG] Ошибка обработки kp_id={kp_id}: {e}", exc_info=True)
                errors.append(f"{kp_id}: {str(e)[:50]}")
        
        # Генерируем deep link
        bot_username = bot.get_me().username
        deep_link = f"https://t.me/{bot_username}?start=tag_{short_code}"
        
        # Формируем ответ
        if existing_tag_id:
            result_text = f"✅ <b>Подборка обновлена!</b>\n\n"
        else:
            result_text = f"✅ <b>Подборка создана!</b>\n\n"
        
        result_text += f"📌 <b>Название:</b> {tag_name}\n"
        result_text += f"🎬 <b>Добавлено новых:</b> {added_count}\n"
        
        if already_in_tag > 0:
            result_text += f"ℹ️ <b>Уже было в подборке:</b> {already_in_tag}\n"
        
        if errors:
            result_text += f"\n⚠️ <b>Ошибки ({len(errors)}):</b>\n"
            for error in errors[:5]:  # Показываем первые 5 ошибок
                result_text += f"• {error}\n"
            if len(errors) > 5:
                result_text += f"• ... и ещё {len(errors) - 5}\n"
        
        result_text += f"\n🔗 <b>Ссылка для добавления:</b>\n"
        result_text += f"<code>{deep_link}</code>"
        
        bot.reply_to(message, result_text, parse_mode='HTML')
        
        # Очищаем состояние
        if user_id in user_add_tag_state:
            del user_add_tag_state[user_id]
            
    except Exception as e:
        logger.error(f"[ADD TAG] Критическая ошибка: {e}", exc_info=True)
        bot.reply_to(message, "❌ Произошла ошибка при обработке подборки.")
        if user_id in user_add_tag_state:
            del user_add_tag_state[user_id]


def is_new_user(user_id, chat_id):
    """Проверяет, является ли пользователь новым (нет записей в stats)"""
    conn = get_db_connection()
    cursor = get_db_cursor()
    try:
        with db_lock:
            cursor.execute('SELECT COUNT(*) FROM stats WHERE user_id = %s AND chat_id = %s', (user_id, chat_id))
            row = cursor.fetchone()
            count = row.get('count') if isinstance(row, dict) else row[0]
            return count == 0
    except Exception as e:
        logger.error(f"[IS NEW USER] Ошибка проверки: {e}", exc_info=True)
        # В случае ошибки считаем пользователя новым для безопасности
        return True
    finally:
        try:
            cursor.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass


def handle_tag_deep_link(bot, message, short_code):
    """Обработчик deep link для добавления фильмов из подборки"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # В private чате chat_id должен быть равен user_id
    if message.chat.type == 'private':
        chat_id = user_id
        logger.info(f"[TAG DEEP LINK] Исправлен chat_id для private чата: {chat_id}")
    
    logger.info(f"[TAG DEEP LINK] Обработка для user_id={user_id}, chat_id={chat_id}, code={short_code}")
    
    # Проверяем, является ли пользователь новым
    is_new = is_new_user(user_id, chat_id)
    logger.info(f"[TAG DEEP LINK] Пользователь user_id={user_id} новый: {is_new}")
    
    # Если пользователь новый, приветствие уже показано в start.py, просто продолжаем обработку deep link
    
    # Отправляем сообщение о загрузке
    loading_msg = bot.reply_to(message, "⏳ Загружаю подборку...")
    loading_msg_id = loading_msg.message_id if loading_msg else None
    
    # Получаем информацию о теге
    conn = get_db_connection()
    cursor = get_db_cursor()
    tag_info = None
    tag_movies = []
    films_list = []
    series_list = []
    
    try:
        with db_lock:
            cursor.execute('SELECT id, name FROM tags WHERE short_code = %s', (short_code,))
            row = cursor.fetchone()
            if row:
                tag_info = {
                    'id': row.get('id') if isinstance(row, dict) else row[0],
                    'name': row.get('name') if isinstance(row, dict) else row[1]
                }
                
                # Получаем список фильмов из подборки
                cursor.execute('''
                    SELECT kp_id, is_series 
                    FROM tag_movies 
                    WHERE tag_id = %s
                    ORDER BY added_at
                ''', (tag_info['id'],))
                rows = cursor.fetchall()
                tag_movies = []
                for row_item in rows:
                    if isinstance(row_item, dict):
                        tag_movies.append((row_item.get('kp_id'), row_item.get('is_series')))
                    else:
                        tag_movies.append((row_item[0], row_item[1]))
                
                # Получаем названия фильмов из админской базы (быстро, без API запросов)
                ADMIN_CHAT_ID = 301810276
                kp_ids = [kp_id for kp_id, _ in tag_movies[:20]]
                if kp_ids:
                    # Получаем все названия одним запросом из админской базы
                    placeholders = ','.join(['%s'] * len(kp_ids))
                    cursor.execute(f'''
                        SELECT kp_id, title, is_series 
                        FROM movies 
                        WHERE chat_id = %s AND kp_id IN ({placeholders})
                    ''', [ADMIN_CHAT_ID] + kp_ids)
                    title_rows = cursor.fetchall()
                    titles_dict = {}
                    for title_row in title_rows:
                        if isinstance(title_row, dict):
                            kp_id = str(title_row.get('kp_id'))
                            title = title_row.get('title')
                            is_series = bool(title_row.get('is_series', 0))
                        else:
                            kp_id = str(title_row[0])
                            title = title_row[1]
                            is_series = bool(title_row[2] if len(title_row) > 2 else 0)
                        titles_dict[kp_id] = (title, is_series)
                    
                    # Формируем списки
                    for kp_id, is_series in tag_movies[:20]:
                        kp_id_str = str(kp_id)
                        if kp_id_str in titles_dict:
                            title, actual_is_series = titles_dict[kp_id_str]
                            if actual_is_series or is_series:
                                series_list.append(title)
                            else:
                                films_list.append(title)
    except Exception as e:
        logger.error(f"[TAG DEEP LINK] Ошибка получения тега: {e}", exc_info=True)
        bot.reply_to(message, "❌ Ошибка при загрузке подборки.")
        return
    finally:
        try:
            cursor.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass
    
    if not tag_info:
        if loading_msg_id:
            try:
                bot.delete_message(chat_id, loading_msg_id)
            except:
                pass
        bot.reply_to(message, "❌ Подборка не найдена.")
        return
    
    if not tag_movies:
        if loading_msg_id:
            try:
                bot.delete_message(chat_id, loading_msg_id)
            except:
                pass
        bot.reply_to(message, f"❌ Подборка '{tag_info['name']}' пуста.")
        return
    
    # Показываем подтверждение
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ Добавить в базу", callback_data=f"tag_confirm:{short_code}"))
    markup.add(InlineKeyboardButton("❌ Отмена", callback_data="tag_cancel"))
    
    films_count = len([m for m in tag_movies if not m[1]])
    series_count = len([m for m in tag_movies if m[1]])
    
    text = f"📦 <b>Подборка: {tag_info['name']}</b>\n\n"
    text += f"🎬 Фильмов: {films_count}\n"
    text += f"📺 Сериалов: {series_count}\n\n"
    
    # Добавляем список фильмов и сериалов (только те, что есть в базе)
    if films_list:
        text += "<b>🎬 Фильмы:</b>\n"
        for i, film_title in enumerate(films_list[:10], 1):  # Показываем до 10
            text += f"{i}. {film_title}\n"
        if films_count > len(films_list):
            text += f"... и еще {films_count - len(films_list)} фильмов\n"
        text += "\n"
    
    if series_list:
        text += "<b>📺 Сериалы:</b>\n"
        for i, series_title in enumerate(series_list[:10], 1):  # Показываем до 10
            text += f"{i}. {series_title}\n"
        if series_count > len(series_list):
            text += f"... и еще {series_count - len(series_list)} сериалов\n"
        text += "\n"
    
    text += "Добавить все фильмы и сериалы в вашу базу?"
    
    # Удаляем сообщение о загрузке и отправляем итоговое
    if loading_msg_id:
        try:
            bot.delete_message(chat_id, loading_msg_id)
        except:
            pass
    
    bot.reply_to(message, text, parse_mode='HTML', reply_markup=markup)


@bot.callback_query_handler(func=lambda call: call.data.startswith("tag_add_to_existing:"))
def handle_tag_add_to_existing(call):
    """Обработчик подтверждения добавления фильмов к существующему тегу"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    parts = call.data.split(":")
    tag_id = int(parts[1])
    tag_name = ":".join(parts[2:])  # Название может содержать ":"
    
    logger.info(f"[TAG ADD TO EXISTING] user_id={user_id}, tag_id={tag_id}, tag_name={tag_name}")
    
    try:
        bot.answer_callback_query(call.id, "⏳ Добавляю фильмы...")
        
        # Получаем сохраненные данные
        if user_id not in user_add_tag_state or 'pending_add' not in user_add_tag_state[user_id]:
            bot.edit_message_text("❌ Данные не найдены. Начните заново.", chat_id, call.message.message_id)
            return
        
        pending_data = user_add_tag_state[user_id]['pending_add']
        kp_ids = pending_data['kp_ids']
        short_code = pending_data['short_code']
        
        # Проверяем, что tag_id совпадает
        if pending_data['tag_id'] != tag_id:
            bot.edit_message_text("❌ Ошибка: несоответствие данных.", chat_id, call.message.message_id)
            if user_id in user_add_tag_state:
                del user_add_tag_state[user_id]
            return
        
        # Добавляем фильмы к существующему тегу (только новые, без дублей)
        added_count = 0
        already_in_tag = 0
        errors = []
        
        for kp_id in kp_ids:
            try:
                # Определяем, фильм это или сериал
                link = f"https://www.kinopoisk.ru/film/{kp_id}/"
                info = extract_movie_info(link)
                
                if not info:
                    # Пробуем как сериал
                    link = f"https://www.kinopoisk.ru/series/{kp_id}/"
                    info = extract_movie_info(link)
                
                if not info:
                    errors.append(f"{kp_id}: не удалось получить информацию")
                    continue
                
                is_series = info.get('is_series', False)
                
                # Добавляем фильм в админскую базу для быстрого получения названий
                ADMIN_CHAT_ID = 301810276
                conn_admin = get_db_connection()
                cursor_admin = get_db_cursor()
                try:
                    with db_lock:
                        cursor_admin.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (ADMIN_CHAT_ID, kp_id))
                        if not cursor_admin.fetchone():
                            from moviebot.bot.handlers.series import ensure_movie_in_database
                            ensure_movie_in_database(ADMIN_CHAT_ID, kp_id, link, info, ADMIN_CHAT_ID)
                            logger.info(f"[ADD TAG] Добавлен kp_id={kp_id} в админскую базу для быстрого доступа")
                except Exception as e:
                    logger.warning(f"[ADD TAG] Ошибка добавления в админскую базу kp_id={kp_id}: {e}")
                finally:
                    try:
                        cursor_admin.close()
                    except:
                        pass
                    try:
                        conn_admin.close()
                    except:
                        pass
                
                # Добавляем в tag_movies (проверяем, не добавлен ли уже - дубли не создаем)
                conn_add = get_db_connection()
                cursor_add = get_db_cursor()
                try:
                    with db_lock:
                        # Проверяем, есть ли уже этот фильм в подборке
                        cursor_add.execute('SELECT id FROM tag_movies WHERE tag_id = %s AND kp_id = %s', (tag_id, kp_id))
                        if cursor_add.fetchone():
                            already_in_tag += 1
                            logger.info(f"[ADD TAG] kp_id={kp_id} уже есть в подборке {tag_id}, пропускаем (дубль)")
                        else:
                            cursor_add.execute('''
                                INSERT INTO tag_movies (tag_id, kp_id, is_series)
                                VALUES (%s, %s, %s)
                            ''', (tag_id, kp_id, is_series))
                            conn_add.commit()
                            added_count += 1
                            logger.info(f"[ADD TAG] Добавлен kp_id={kp_id} (is_series={is_series}) в тег {tag_id}")
                except Exception as e:
                    logger.error(f"[ADD TAG] Ошибка добавления kp_id={kp_id}: {e}")
                    errors.append(f"{kp_id}: ошибка БД")
                finally:
                    try:
                        cursor_add.close()
                    except:
                        pass
                    try:
                        conn_add.close()
                    except:
                        pass
                        
            except Exception as e:
                logger.error(f"[ADD TAG] Ошибка обработки kp_id={kp_id}: {e}", exc_info=True)
                errors.append(f"{kp_id}: {str(e)[:50]}")
        
        # Формируем итоговое сообщение
        result_text = f"✅ <b>Фильмы добавлены в подборку '{tag_name}'!</b>\n\n"
        
        if added_count > 0:
            result_text += f"✅ Добавлено новых: <b>{added_count}</b>\n"
        if already_in_tag > 0:
            result_text += f"ℹ️ Пропущено дублей: <b>{already_in_tag}</b>\n"
        if errors:
            result_text += f"❌ Ошибок: <b>{len(errors)}</b>\n"
        
        result_text += f"\n🔗 Ссылка на подборку:\n"
        bot_username = bot.get_me().username
        deep_link = f"https://t.me/{bot_username}?start=tag_{short_code}"
        result_text += f"<code>{deep_link}</code>"
        
        # Очищаем состояние
        if user_id in user_add_tag_state:
            del user_add_tag_state[user_id]
        
        bot.edit_message_text(result_text, chat_id, call.message.message_id, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"[TAG ADD TO EXISTING] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка при добавлении", show_alert=True)
        except:
            pass
        if user_id in user_add_tag_state:
            del user_add_tag_state[user_id]


@bot.callback_query_handler(func=lambda call: call.data == "tag_cancel_add")
def handle_tag_cancel_add(call):
    """Обработчик отмены добавления к существующему тегу"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    try:
        bot.answer_callback_query(call.id, "❌ Отменено")
        bot.edit_message_text("❌ Добавление отменено.", chat_id, call.message.message_id)
        
        if user_id in user_add_tag_state:
            del user_add_tag_state[user_id]
    except Exception as e:
        logger.error(f"[TAG CANCEL ADD] Ошибка: {e}", exc_info=True)


@bot.callback_query_handler(func=lambda call: call.data.startswith("tag_confirm:"))
def handle_tag_confirm(call):
    """Обработчик подтверждения добавления фильмов из подборки"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    # В private чате chat_id должен быть равен user_id
    if call.message.chat.type == 'private':
        chat_id = user_id
        logger.info(f"[TAG CONFIRM] Исправлен chat_id для private чата: {chat_id}")
    
    short_code = call.data.split(":")[1]
    
    logger.info(f"[TAG CONFIRM] user_id={user_id}, chat_id={chat_id}, code={short_code}")
    
    try:
        bot.answer_callback_query(call.id, "⏳ Добавляю фильмы...")
        
        # Отправляем сообщение о загрузке
        loading_msg = bot.send_message(chat_id, "⏳ Загружаю фильмы и сериалы...")
        loading_msg_id = loading_msg.message_id if loading_msg else None
        
        # Получаем информацию о теге и фильмах
        conn = get_db_connection()
        cursor = get_db_cursor()
        tag_info = None
        tag_movies = []
        
        try:
            with db_lock:
                cursor.execute('SELECT id, name FROM tags WHERE short_code = %s', (short_code,))
                row = cursor.fetchone()
                if row:
                    tag_info = {
                        'id': row.get('id') if isinstance(row, dict) else row[0],
                        'name': row.get('name') if isinstance(row, dict) else row[1]
                    }
                    cursor.execute('''
                        SELECT kp_id, is_series 
                        FROM tag_movies 
                        WHERE tag_id = %s
                        ORDER BY added_at
                    ''', (tag_info['id'],))
                    rows = cursor.fetchall()
                    tag_movies = []
                    for row_item in rows:
                        if isinstance(row_item, dict):
                            tag_movies.append((row_item.get('kp_id'), row_item.get('is_series')))
                        else:
                            tag_movies.append((row_item[0], row_item[1]))
        finally:
            try:
                cursor.close()
            except:
                pass
            try:
                conn.close()
            except:
                pass
        
        if not tag_info or not tag_movies:
            bot.edit_message_text("❌ Подборка не найдена.", chat_id, call.message.message_id)
            return
        
        # Добавляем фильмы в базу пользователя
        added_films = []
        added_series = []
        already_in_db = []
        already_watched = []
        already_planned = []
        errors = []
        
        total_movies = len(tag_movies)
        logger.info(f"[TAG CONFIRM] Начинаем обработку {total_movies} фильмов для user_id={user_id}, chat_id={chat_id}, tag_id={tag_info['id']}")
        for idx, (kp_id, is_series) in enumerate(tag_movies, 1):
            # Обновляем сообщение о загрузке каждые 5 фильмов
            if loading_msg_id and idx % 5 == 0:
                try:
                    progress = int((idx / total_movies) * 100)
                    bot.edit_message_text(
                        f"⏳ Загружаю фильмы и сериалы... {progress}% ({idx}/{total_movies})",
                        chat_id, loading_msg_id
                    )
                except:
                    pass
            
            try:
                link = f"https://www.kinopoisk.ru/series/{kp_id}/" if is_series else f"https://www.kinopoisk.ru/film/{kp_id}/"
                info = extract_movie_info(link)
                
                if not info:
                    errors.append(f"{kp_id}: не удалось получить информацию")
                    continue
                
                title = info.get('title', f'Фильм {kp_id}')
                
                # Проверяем, есть ли уже в базе
                conn_check = get_db_connection()
                cursor_check = get_db_cursor()
                film_id = None
                is_watched = False
                has_plan = False
                
                try:
                    with db_lock:
                        cursor_check.execute('''
                            SELECT id, watched 
                            FROM movies 
                            WHERE chat_id = %s AND kp_id = %s
                        ''', (chat_id, kp_id))
                        row = cursor_check.fetchone()
                        if row:
                            film_id = row[0] if isinstance(row, tuple) else row.get('id')
                            is_watched = bool(row[1] if isinstance(row, tuple) else row.get('watched'))
                            
                            # Проверяем планы
                            if film_id:
                                cursor_check.execute('SELECT id FROM plans WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
                                has_plan = cursor_check.fetchone() is not None
                finally:
                    try:
                        cursor_check.close()
                    except:
                        pass
                    try:
                        conn_check.close()
                    except:
                        pass
                
                if film_id:
                    # Уже в базе
                    already_in_db.append((title, is_watched, has_plan))
                    logger.info(f"[TAG CONFIRM] Фильм уже в базе: kp_id={kp_id}, film_id={film_id}, title={title}")
                    # Записываем связь с тегом
                    conn_link = get_db_connection()
                    cursor_link = get_db_cursor()
                    try:
                        with db_lock:
                            cursor_link.execute('''
                                INSERT INTO user_tag_movies (user_id, chat_id, tag_id, film_id)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (user_id, chat_id, tag_id, film_id) DO NOTHING
                            ''', (user_id, chat_id, tag_info['id'], film_id))
                            conn_link.commit()
                            logger.info(f"[TAG CONFIRM] Добавлена запись в user_tag_movies (уже в базе): user_id={user_id}, chat_id={chat_id}, tag_id={tag_info['id']}, film_id={film_id}")
                    finally:
                        try:
                            cursor_link.close()
                        except:
                            pass
                        try:
                            conn_link.close()
                        except:
                            pass
                else:
                    # Добавляем фильм
                    logger.info(f"[TAG CONFIRM] Фильм не найден в базе, добавляем: kp_id={kp_id}, title={title}")
                    film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
                    logger.info(f"[TAG CONFIRM] Результат ensure_movie_in_database: film_id={film_id}, was_inserted={was_inserted}")
                    if film_id:
                        if is_series:
                            added_series.append(title)
                        else:
                            added_films.append(title)
                        
                        # Записываем связь с тегом
                        conn_link = get_db_connection()
                        cursor_link = get_db_cursor()
                        try:
                            with db_lock:
                                cursor_link.execute('''
                                    INSERT INTO user_tag_movies (user_id, chat_id, tag_id, film_id)
                                    VALUES (%s, %s, %s, %s)
                                    ON CONFLICT (user_id, chat_id, tag_id, film_id) DO NOTHING
                                ''', (user_id, chat_id, tag_info['id'], film_id))
                                conn_link.commit()
                                logger.info(f"[TAG CONFIRM] Добавлена запись в user_tag_movies (новый фильм): user_id={user_id}, chat_id={chat_id}, tag_id={tag_info['id']}, film_id={film_id}")
                        finally:
                            try:
                                cursor_link.close()
                            except:
                                pass
                            try:
                                conn_link.close()
                            except:
                                pass
                    else:
                        errors.append(f"{title}: не удалось добавить")
                        
            except Exception as e:
                logger.error(f"[TAG CONFIRM] Ошибка обработки kp_id={kp_id}: {e}", exc_info=True)
                errors.append(f"{kp_id}: {str(e)[:50]}")
        
        # Удаляем сообщение о загрузке
        if loading_msg_id:
            try:
                bot.delete_message(chat_id, loading_msg_id)
            except:
                pass
        
        # Формируем итоговое сообщение
        result_text = f"✅ <b>Подборка '{tag_info['name']}' добавлена!</b>\n\n"
        
        if added_films or added_series:
            result_text += f"🎬 <b>Добавлено фильмов:</b> {len(added_films)}\n"
            result_text += f"📺 <b>Добавлено сериалов:</b> {len(added_series)}\n\n"
        
        if already_in_db:
            watched_count = len([x for x in already_in_db if x[1]])
            planned_count = len([x for x in already_in_db if x[2]])
            result_text += f"ℹ️ <b>Уже в базе:</b> {len(already_in_db)}\n"
            if watched_count > 0:
                result_text += f"   ✅ Просмотрено: {watched_count}\n"
            if planned_count > 0:
                result_text += f"   📅 Запланировано: {planned_count}\n"
            result_text += "\n"
        
        if errors:
            result_text += f"⚠️ <b>Ошибки ({len(errors)}):</b>\n"
            for error in errors[:3]:
                result_text += f"• {error}\n"
            if len(errors) > 3:
                result_text += f"• ... и ещё {len(errors) - 3}\n"
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🏷️ Посмотреть подборку", callback_data=f"tag_view:{tag_info['id']}"))
        
        # Проверяем, есть ли общие группы у пользователя и бота для кнопки "Добавить в группу"
        common_groups = []
        conn_groups = get_db_connection()
        cursor_groups = get_db_cursor()
        try:
            with db_lock:
                # Получаем все чаты, где есть подписки пользователя
                cursor_groups.execute('''
                    SELECT DISTINCT chat_id 
                    FROM subscriptions 
                    WHERE user_id = %s AND chat_id < 0
                ''', (user_id,))
                user_groups = [row[0] if isinstance(row, tuple) else row.get('chat_id') for row in cursor_groups.fetchall()]
                
                # Проверяем, в каких из этих групп есть бот
                for group_id in user_groups:
                    try:
                        chat = bot.get_chat(group_id)
                        if chat.type in ['group', 'supergroup']:
                            # Проверяем, что бот является участником
                            try:
                                member = bot.get_chat_member(group_id, bot.get_me().id)
                                if member.status in ['member', 'administrator', 'creator']:
                                    common_groups.append((group_id, chat.title or f"Группа {group_id}"))
                            except:
                                pass
                    except Exception as e:
                        logger.warning(f"[TAG ADD TO GROUP] Ошибка проверки группы {group_id}: {e}")
                        continue
        except Exception as e:
            logger.error(f"[TAG ADD TO GROUP] Ошибка получения списка групп: {e}", exc_info=True)
        finally:
            try:
                cursor_groups.close()
            except:
                pass
            try:
                conn_groups.close()
            except:
                pass
        
        if common_groups:
            # Добавляем кнопку "Добавить в группу"
            markup.add(InlineKeyboardButton("📢 Добавить в группу", callback_data=f"tag_add_to_group:{tag_info['id']}"))
        
        markup.add(InlineKeyboardButton("◀️ В базу", callback_data="back_to_database"))
        
        bot.edit_message_text(result_text, chat_id, call.message.message_id, parse_mode='HTML', reply_markup=markup)
        
    except Exception as e:
        logger.error(f"[TAG CONFIRM] Критическая ошибка: {e}", exc_info=True)
        try:
            bot.edit_message_text("❌ Произошла ошибка при добавлении фильмов.", chat_id, call.message.message_id)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data == "tag_cancel")
def handle_tag_cancel(call):
    """Обработчик отмены добавления подборки"""
    try:
        bot.answer_callback_query(call.id)
        bot.edit_message_text("❌ Добавление отменено.", call.message.chat.id, call.message.message_id)
    except:
        pass


@bot.message_handler(commands=['tags'])
def tags_command(message):
    """Команда /tags - список подборок"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    logger.info(f"[TAGS] Команда /tags от user_id={user_id}, chat_id={chat_id}")
    
    # В private чате chat_id должен быть равен user_id
    if message.chat.type == 'private':
        chat_id = user_id
        logger.info(f"[TAGS] Исправлен chat_id для private чата: {chat_id}")
    
    # Получаем список всех подборок (не только тех, где есть фильмы у пользователя)
    # Но показываем количество фильмов у пользователя в каждой подборке
    conn = get_db_connection()
    cursor = get_db_cursor()
    tags_list = []
    
    try:
        with db_lock:
            # Получаем все подборки, где у пользователя есть хотя бы одна запись в user_tag_movies
            # Считаем фильмы, которые существуют в movies (m.id IS NOT NULL) для правильного chat_id
            # НО показываем подборку, даже если не все фильмы еще добавлены в movies
            logger.info(f"[TAGS] Выполняем SQL запрос с параметрами: user_id={user_id}, chat_id={chat_id}")
            cursor.execute('''
                SELECT 
                    t.id, 
                    t.name,
                    COUNT(DISTINCT CASE WHEN m.id IS NOT NULL THEN utm.film_id END) as user_films_count,
                    (SELECT COUNT(DISTINCT kp_id) FROM tag_movies WHERE tag_id = t.id) as total_films_count,
                    COUNT(DISTINCT CASE WHEN m.id IS NOT NULL AND m.watched = 1 THEN utm.film_id END) as watched_films_count,
                    COUNT(DISTINCT utm.film_id) as total_user_tag_films
                FROM tags t
                INNER JOIN user_tag_movies utm ON t.id = utm.tag_id AND utm.user_id = %s AND utm.chat_id = %s
                LEFT JOIN movies m ON utm.film_id = m.id AND m.chat_id = %s
                GROUP BY t.id, t.name
                ORDER BY t.name
            ''', (user_id, chat_id, chat_id))
            tags_list = cursor.fetchall()
            logger.info(f"[TAGS] Найдено подборок для user_id={user_id}, chat_id={chat_id}: {len(tags_list)}")
            if tags_list:
                for tag_row in tags_list:
                    tag_id = tag_row[0] if isinstance(tag_row, tuple) else tag_row.get('id')
                    tag_name = tag_row[1] if isinstance(tag_row, tuple) else tag_row.get('name')
                    user_films_count = tag_row[2] if isinstance(tag_row, tuple) else tag_row.get('user_films_count')
                    logger.info(f"[TAGS] Найдена подборка: id={tag_id}, name={tag_name}, user_films_count={user_films_count}")
            
            # Если не найдено подборок, проверяем, есть ли вообще записи в user_tag_movies
            if not tags_list:
                cursor.execute('''
                    SELECT COUNT(*) FROM user_tag_movies 
                    WHERE user_id = %s AND chat_id = %s
                ''', (user_id, chat_id))
                count_row = cursor.fetchone()
                count = count_row[0] if isinstance(count_row, tuple) else count_row.get('count', 0)
                logger.info(f"[TAGS] DEBUG: Записей в user_tag_movies для user_id={user_id}, chat_id={chat_id}: {count}")
                
                # Проверяем, какие теги есть в user_tag_movies
                cursor.execute('''
                    SELECT DISTINCT tag_id FROM user_tag_movies 
                    WHERE user_id = %s AND chat_id = %s
                ''', (user_id, chat_id))
                tag_ids = cursor.fetchall()
                logger.info(f"[TAGS] DEBUG: Теги в user_tag_movies: {[row[0] if isinstance(row, tuple) else row.get('tag_id') for row in tag_ids]}")
                
                # Проверяем, есть ли записи с другими user_id или chat_id
                cursor.execute('''
                    SELECT DISTINCT user_id, chat_id, COUNT(*) as cnt
                    FROM user_tag_movies 
                    WHERE tag_id IN (SELECT DISTINCT tag_id FROM user_tag_movies WHERE user_id = %s AND chat_id = %s)
                    GROUP BY user_id, chat_id
                ''', (user_id, chat_id))
                all_records = cursor.fetchall()
                logger.info(f"[TAGS] DEBUG: Все записи для этих тегов: {[(r[0] if isinstance(r, tuple) else r.get('user_id'), r[1] if isinstance(r, tuple) else r.get('chat_id'), r[2] if isinstance(r, tuple) else r.get('cnt')) for r in all_records]}")
                
                # Проверяем, есть ли фильмы в movies для этих film_id
                if tag_ids:
                    tag_id_list = [row[0] if isinstance(row, tuple) else row.get('tag_id') for row in tag_ids]
                    for tid in tag_id_list:
                        cursor.execute('''
                            SELECT utm.film_id, m.id as movie_id, m.chat_id as movie_chat_id
                            FROM user_tag_movies utm
                            LEFT JOIN movies m ON utm.film_id = m.id
                            WHERE utm.user_id = %s AND utm.chat_id = %s AND utm.tag_id = %s
                            LIMIT 5
                        ''', (user_id, chat_id, tid))
                        films = cursor.fetchall()
                        logger.info(f"[TAGS] DEBUG: Для тега {tid} найдено записей: {len(films)}")
                        for film_row in films:
                            film_id = film_row[0] if isinstance(film_row, tuple) else film_row.get('film_id')
                            movie_id = film_row[1] if isinstance(film_row, tuple) else film_row.get('movie_id')
                            movie_chat_id = film_row[2] if isinstance(film_row, tuple) else film_row.get('movie_chat_id')
                            logger.info(f"[TAGS] DEBUG: film_id={film_id}, movie_id={movie_id}, movie_chat_id={movie_chat_id}, expected_chat_id={chat_id}")
    except Exception as e:
        logger.error(f"[TAGS] Ошибка получения списка подборок: {e}", exc_info=True)
    finally:
        try:
            cursor.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass
    
    if not tags_list:
        text = "🏷️ <b>Подборки</b>\n\nПока что подборок не добавлено, следите за кино пабликами и новостями!"
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("🔍 Найти фильм", callback_data="start_menu:search"))
        markup.add(InlineKeyboardButton("◀️ Назад в базу", callback_data="back_to_database"))
        bot.reply_to(message, text, parse_mode='HTML', reply_markup=markup)
        return
    
    text = "🏷️ <b>Тут собраны все добавленные подборки</b>\n\n"
    markup = InlineKeyboardMarkup(row_width=1)
    
    # Разделяем на просмотренные и непросмотренные
    unwatched_tags = []
    watched_tags = []
    
    for tag_row in tags_list:
        tag_id = tag_row[0] if isinstance(tag_row, tuple) else tag_row.get('id')
        tag_name = tag_row[1] if isinstance(tag_row, tuple) else tag_row.get('name')
        user_films_count = tag_row[2] if isinstance(tag_row, tuple) else tag_row.get('user_films_count', 0)
        total_films_count = tag_row[3] if isinstance(tag_row, tuple) else tag_row.get('total_films_count', 0)
        watched_films_count = tag_row[4] if isinstance(tag_row, tuple) else tag_row.get('watched_films_count', 0)
        
        # Если у пользователя есть фильмы в теге и все они просмотрены - тег просмотрен
        is_watched = user_films_count > 0 and watched_films_count == user_films_count
        
        tag_info = {
            'id': tag_id,
            'name': tag_name,
            'user_films_count': user_films_count,
            'total_films_count': total_films_count,
            'watched_films_count': watched_films_count,
            'is_watched': is_watched
        }
        
        if is_watched:
            watched_tags.append(tag_info)
        else:
            unwatched_tags.append(tag_info)
    
    # Сначала показываем непросмотренные
    for tag_info in unwatched_tags:
        count_text = f"{tag_info['user_films_count']}" if tag_info['user_films_count'] > 0 else f"0/{tag_info['total_films_count']}"
        button_text = f"📦 {tag_info['name']} ({count_text})"
        if len(button_text) > 60:
            button_text = button_text[:57] + "..."
        markup.add(InlineKeyboardButton(button_text, callback_data=f"tag_view:{tag_info['id']}"))
    
    # Кнопка "✅ Просмотренные" если есть просмотренные теги
    if watched_tags:
        watched_count = len(watched_tags)
        watched_button_text = f"✅ Просмотренные ({watched_count})"
        markup.add(InlineKeyboardButton(watched_button_text, callback_data="watched_tags_list"))
    
    markup.add(InlineKeyboardButton("◀️ Назад в базу", callback_data="back_to_database"))
    
    bot.reply_to(message, text, parse_mode='HTML', reply_markup=markup)


# Состояние для пагинации тегов
user_tag_list_state = {}


def show_tag_films_page(bot, chat_id, user_id, tag_id, page=1, message_id=None):
    """Показывает страницу фильмов из подборки (аналогично show_list_page)"""
    MOVIES_PER_PAGE = 15  # Как в /list
    
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        # Получаем информацию о теге
        tag_name = None
        with db_lock:
            cursor_local.execute('SELECT name FROM tags WHERE id = %s', (tag_id,))
            row = cursor_local.fetchone()
            if row:
                tag_name = row[0] if isinstance(row, tuple) else row.get('name')
        
        if not tag_name:
            text = "❌ Подборка не найдена."
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("◀️ Назад к подборкам", callback_data="tags_list"))
            if message_id:
                bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
            else:
                bot.send_message(chat_id, text, reply_markup=markup)
            return
        
        # Получаем фильмы из подборки
        with db_lock:
            cursor_local.execute('''
                SELECT m.id, m.kp_id, m.title, m.year, m.genres, m.link, m.watched, m.is_series,
                       COALESCE(AVG(r.rating), 0) as avg_rating
                FROM user_tag_movies utm
                INNER JOIN movies m ON utm.film_id = m.id
                LEFT JOIN ratings r ON m.id = r.film_id AND r.chat_id = %s 
                    AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                WHERE utm.user_id = %s AND utm.chat_id = %s AND utm.tag_id = %s
                GROUP BY m.id, m.kp_id, m.title, m.year, m.genres, m.link, m.watched, m.is_series
                ORDER BY m.watched ASC, m.title
            ''', (chat_id, user_id, chat_id, tag_id))
            rows = cursor_local.fetchall()
        
        if not rows:
            text = f"📦 <b>{tag_name}</b>\n\nВ этой подборке пока нет фильмов в вашей базе."
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("◀️ Назад к подборкам", callback_data="tags_list"))
            if message_id:
                bot.edit_message_text(text, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
            else:
                bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup)
            return
        
        # Подсчитываем просмотренные
        watched_count = len([r for r in rows if (r[6] if isinstance(r, tuple) else r.get('watched'))])
        total_count = len(rows)
        
        # Разделяем на просмотренные и непросмотренные
        unwatched = [r for r in rows if not (r[6] if isinstance(r, tuple) else r.get('watched'))]
        watched = [r for r in rows if (r[6] if isinstance(r, tuple) else r.get('watched'))]
        
        # Объединяем: сначала непросмотренные, потом просмотренные
        all_films = unwatched + watched
        total_movies = len(all_films)
        total_pages = (total_movies + MOVIES_PER_PAGE - 1) // MOVIES_PER_PAGE
        page = max(1, min(page, total_pages))
        
        # Вычисляем диапазон фильмов для текущей страницы
        start_idx = (page - 1) * MOVIES_PER_PAGE
        end_idx = min(start_idx + MOVIES_PER_PAGE, total_movies)
        page_movies = all_films[start_idx:end_idx]
        
        # Формируем текст страницы
        text = f"📦 <b>{tag_name}</b>\n\n"
        text += f"Просмотрено: {watched_count}/{total_count}\n\n"
        if total_pages > 1:
            text += f"Страница {page}/{total_pages}:\n\n"
        
        for row in page_movies:
            film_id = row.get('id') if isinstance(row, dict) else row[0]
            kp_id = row.get('kp_id') if isinstance(row, dict) else (row[1] if len(row) > 1 else None)
            title = row.get('title') if isinstance(row, dict) else row[2]
            year = row.get('year') if isinstance(row, dict) else (row[3] if len(row) > 3 else '—')
            genres = row.get('genres') if isinstance(row, dict) else (row[4] if len(row) > 4 else None)
            link = row.get('link') if isinstance(row, dict) else (row[5] if len(row) > 5 else '')
            is_watched = row.get('watched') if isinstance(row, dict) else row[6]
            avg_rating = row.get('avg_rating') if isinstance(row, dict) else (row[8] if len(row) > 8 else 0)
            
            # Извлекаем первый жанр
            first_genre = None
            if genres and genres != '—' and genres.strip():
                genres_list = [g.strip() for g in genres.split(',')]
                if genres_list:
                    first_genre = genres_list[0]
            
            movie_id = kp_id or film_id
            genre_str = f" • {first_genre}" if first_genre else ""
            year_str = f" ({year})" if year and year != '—' and str(year).lower() != 'none' else ""
            
            watched_marker = "✅ " if is_watched else ""
            rating_text = f" — {avg_rating:.1f}/10" if avg_rating and avg_rating > 0 else ""
            
            text += f"{watched_marker}• <b>{title}</b>{year_str}{genre_str}{rating_text} [ID: {movie_id}]\n<a href='{link}'>{link}</a>\n\n"
        
        # Создаем кнопки пагинации (как в /list)
        markup = InlineKeyboardMarkup()
        
        if total_pages > 1:
            pagination_buttons = []
            
            if total_pages <= 20:
                for p in range(1, total_pages + 1):
                    label = f"•{p}" if p == page else str(p)
                    pagination_buttons.append(InlineKeyboardButton(label, callback_data=f"tag_page:{tag_id}:{p}"))
                for i in range(0, len(pagination_buttons), 10):
                    markup.row(*pagination_buttons[i:i+10])
            else:
                start_page = max(1, page - 2)
                end_page = min(total_pages, page + 2)
                
                if start_page > 2:
                    pagination_buttons.append(InlineKeyboardButton("1", callback_data=f"tag_page:{tag_id}:1"))
                    pagination_buttons.append(InlineKeyboardButton("...", callback_data="noop"))
                elif start_page == 2:
                    pagination_buttons.append(InlineKeyboardButton("1", callback_data=f"tag_page:{tag_id}:1"))
                
                for p in range(start_page, end_page + 1):
                    label = f"•{p}" if p == page else str(p)
                    pagination_buttons.append(InlineKeyboardButton(label, callback_data=f"tag_page:{tag_id}:{p}"))
                
                if end_page < total_pages - 1:
                    pagination_buttons.append(InlineKeyboardButton("...", callback_data="noop"))
                    pagination_buttons.append(InlineKeyboardButton(str(total_pages), callback_data=f"tag_page:{tag_id}:{total_pages}"))
                elif end_page < total_pages:
                    pagination_buttons.append(InlineKeyboardButton(str(total_pages), callback_data=f"tag_page:{tag_id}:{total_pages}"))
                
                for i in range(0, len(pagination_buttons), 10):
                    markup.row(*pagination_buttons[i:i+10])
            
            nav_buttons = []
            if page > 1:
                nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"tag_page:{tag_id}:{page-1}"))
            if page < total_pages:
                nav_buttons.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"tag_page:{tag_id}:{page+1}"))
            if nav_buttons:
                markup.row(*nav_buttons)
        
        # Добавляем кнопки действий (каждая в отдельном ряду)
        markup.add(InlineKeyboardButton("📖 Перейти к описанию", callback_data=f"view_film_from_tag:{tag_id}"))
        markup.add(InlineKeyboardButton("📅 Запланировать просмотр", callback_data=f"plan_from_tag:{tag_id}"))
        markup.add(InlineKeyboardButton("👁️ Отметить просмотренным", callback_data=f"mark_watched_from_tag:{tag_id}"))
        markup.add(InlineKeyboardButton("◀️ Назад в базу", callback_data="back_to_database"))
        
        # Сохраняем состояние
        user_tag_list_state[user_id] = {
            'tag_id': tag_id,
            'page': page,
            'total_pages': total_pages,
            'chat_id': chat_id
        }
        
        if message_id:
            try:
                bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True)
            except Exception as e:
                logger.error(f"[TAG FILMS] Ошибка редактирования: {e}", exc_info=True)
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True)
        else:
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True)
            
    except Exception as e:
        logger.error(f"[TAG FILMS] Ошибка: {e}", exc_info=True)
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("tag_page:"))
def handle_tag_page(call):
    """Обработчик переключения страниц в подборке"""
    user_id = call.from_user.id
    
    try:
        bot.answer_callback_query(call.id)
        
        parts = call.data.split(":")
        tag_id = int(parts[1])
        page = int(parts[2])
        
        state = user_tag_list_state.get(user_id)
        if not state or state.get('tag_id') != tag_id:
            bot.answer_callback_query(call.id, "Сессия устарела. Откройте подборку заново", show_alert=True)
            return
        
        chat_id = state['chat_id']
        show_tag_films_page(bot, chat_id, user_id, tag_id, page, call.message.message_id)
        
    except Exception as e:
        logger.error(f"[TAG PAGE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка переключения страницы")
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("tag_view:"))
def handle_tag_view(call):
    """Обработчик просмотра подборки (список фильмов)"""
    user_id = call.from_user.id
    tag_id = int(call.data.split(":")[1])
    
    logger.info(f"[TAG VIEW] user_id={user_id}, tag_id={tag_id}")
    
    try:
        bot.answer_callback_query(call.id, "⏳ Загружаю...")
        show_tag_films_page(bot, call.message.chat.id, user_id, tag_id, page=1, message_id=call.message.message_id)
    except Exception as e:
        logger.error(f"[TAG VIEW] Ошибка: {e}", exc_info=True)
        try:
            bot.edit_message_text("❌ Ошибка при загрузке подборки.", call.message.chat.id, call.message.message_id)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data == "watched_tags_list")
def handle_watched_tags_list(call):
    """Обработчик кнопки '✅ Просмотренные' для тегов"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    
    logger.info(f"[WATCHED TAGS] Показ просмотренных тегов для user_id={user_id}")
    
    try:
        safe_answer_callback_query(bot, call.id)
        
        conn = get_db_connection()
        cursor = get_db_cursor()
        watched_tags_list = []
        
        try:
            with db_lock:
                # Получаем все подборки, где все фильмы пользователя просмотрены
                # Считаем только фильмы, которые существуют в movies (m.id IS NOT NULL)
                cursor.execute('''
                    SELECT DISTINCT t.id, t.name,
                           COALESCE(COUNT(DISTINCT CASE WHEN utm.film_id IS NOT NULL AND m.id IS NOT NULL THEN utm.film_id END), 0) as user_films_count,
                           COUNT(DISTINCT tm.kp_id) as total_films_count,
                           COALESCE(COUNT(DISTINCT CASE WHEN utm.film_id IS NOT NULL AND m.id IS NOT NULL AND m.watched = 1 THEN utm.film_id END), 0) as watched_films_count
                    FROM tags t
                    INNER JOIN user_tag_movies utm ON t.id = utm.tag_id AND utm.user_id = %s AND utm.chat_id = %s
                    LEFT JOIN tag_movies tm ON t.id = tm.tag_id
                    LEFT JOIN movies m ON utm.film_id = m.id AND m.chat_id = %s
                    GROUP BY t.id, t.name
                    HAVING COALESCE(COUNT(DISTINCT CASE WHEN utm.film_id IS NOT NULL AND m.id IS NOT NULL AND m.watched = 1 THEN utm.film_id END), 0) = 
                           COALESCE(COUNT(DISTINCT CASE WHEN utm.film_id IS NOT NULL AND m.id IS NOT NULL THEN utm.film_id END), 0)
                       AND COALESCE(COUNT(DISTINCT CASE WHEN utm.film_id IS NOT NULL AND m.id IS NOT NULL THEN utm.film_id END), 0) > 0
                    ORDER BY t.name
                ''', (user_id, chat_id, chat_id))
                watched_tags_list = cursor.fetchall()
        except Exception as e:
            logger.error(f"[WATCHED TAGS] Ошибка получения списка просмотренных тегов: {e}", exc_info=True)
        finally:
            try:
                cursor.close()
            except:
                pass
            try:
                conn.close()
            except:
                pass
        
        if not watched_tags_list:
            text = "✅ <b>Просмотренные подборки</b>\n\nПока что нет полностью просмотренных подборок."
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("◀️ Назад к подборкам", callback_data="tags_list"))
            bot.edit_message_text(text, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
            return
        
        text = f"✅ <b>Просмотренные подборки</b>\n\nНайдено: {len(watched_tags_list)}\n\n"
        markup = InlineKeyboardMarkup(row_width=1)
        
        for tag_row in watched_tags_list:
            tag_id = tag_row[0] if isinstance(tag_row, tuple) else tag_row.get('id')
            tag_name = tag_row[1] if isinstance(tag_row, tuple) else tag_row.get('name')
            user_films_count = tag_row[2] if isinstance(tag_row, tuple) else tag_row.get('user_films_count', 0)
            
            button_text = f"✅ {tag_name} ({user_films_count})"
            if len(button_text) > 60:
                button_text = button_text[:57] + "..."
            markup.add(InlineKeyboardButton(button_text, callback_data=f"tag_view:{tag_id}"))
        
        markup.add(InlineKeyboardButton("◀️ Назад к подборкам", callback_data="tags_list"))
        
        bot.edit_message_text(text, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
        
    except Exception as e:
        logger.error(f"[WATCHED TAGS] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data == "tags_list")
def handle_tags_list(call):
    """Обработчик возврата к списку подборок"""
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # В private чате chat_id должен быть равен user_id
        if call.message.chat.type == 'private':
            chat_id = user_id
        
        # Получаем список всех подборок
        conn = get_db_connection()
        cursor = get_db_cursor()
        tags_list = []
        
        try:
            with db_lock:
                # Получаем все подборки, где у пользователя есть хотя бы одна запись в user_tag_movies
                # Считаем только фильмы, которые существуют в movies (m.id IS NOT NULL)
                cursor.execute('''
                    SELECT DISTINCT t.id, t.name,
                           COALESCE(COUNT(DISTINCT CASE WHEN utm.film_id IS NOT NULL AND m.id IS NOT NULL THEN utm.film_id END), 0) as user_films_count,
                           COUNT(DISTINCT tm.kp_id) as total_films_count,
                           COALESCE(COUNT(DISTINCT CASE WHEN utm.film_id IS NOT NULL AND m.id IS NOT NULL AND m.watched = 1 THEN utm.film_id END), 0) as watched_films_count
                    FROM tags t
                    INNER JOIN user_tag_movies utm ON t.id = utm.tag_id AND utm.user_id = %s AND utm.chat_id = %s
                    LEFT JOIN tag_movies tm ON t.id = tm.tag_id
                    LEFT JOIN movies m ON utm.film_id = m.id AND m.chat_id = %s
                    GROUP BY t.id, t.name
                    ORDER BY t.name
                ''', (user_id, chat_id, chat_id))
                tags_list = cursor.fetchall()
        except Exception as e:
            logger.error(f"[TAGS LIST] Ошибка получения списка подборок: {e}", exc_info=True)
        finally:
            try:
                cursor.close()
            except:
                pass
            try:
                conn.close()
            except:
                pass
        
        if not tags_list:
            text = "🏷️ <b>Подборки</b>\n\nПока что подборок не добавлено, следите за кино пабликами и новостями!"
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("🔍 Найти фильм", callback_data="start_menu:search"))
            markup.add(InlineKeyboardButton("◀️ Назад в базу", callback_data="back_to_database"))
            bot.edit_message_text(text, chat_id, call.message.message_id, parse_mode='HTML', reply_markup=markup)
            return
        
        text = "🏷️ <b>Подборки</b>\n\nТут собраны все добавленные подборки\n\n"
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Разделяем на просмотренные и непросмотренные
        unwatched_tags = []
        watched_tags = []
        
        for tag_row in tags_list:
            tag_id = tag_row[0] if isinstance(tag_row, tuple) else tag_row.get('id')
            tag_name = tag_row[1] if isinstance(tag_row, tuple) else tag_row.get('name')
            user_films_count = tag_row[2] if isinstance(tag_row, tuple) else tag_row.get('user_films_count', 0)
            total_films_count = tag_row[3] if isinstance(tag_row, tuple) else tag_row.get('total_films_count', 0)
            watched_films_count = tag_row[4] if isinstance(tag_row, tuple) else tag_row.get('watched_films_count', 0)
            
            # Если у пользователя есть фильмы в теге и все они просмотрены - тег просмотрен
            is_watched = user_films_count > 0 and watched_films_count == user_films_count
            
            tag_info = {
                'id': tag_id,
                'name': tag_name,
                'user_films_count': user_films_count,
                'total_films_count': total_films_count,
                'watched_films_count': watched_films_count,
                'is_watched': is_watched
            }
            
            if is_watched:
                watched_tags.append(tag_info)
            else:
                unwatched_tags.append(tag_info)
        
        # Сначала показываем непросмотренные
        for tag_info in unwatched_tags:
            count_text = f"{tag_info['user_films_count']}" if tag_info['user_films_count'] > 0 else f"0/{tag_info['total_films_count']}"
            button_text = f"📦 {tag_info['name']} ({count_text})"
            if len(button_text) > 60:
                button_text = button_text[:57] + "..."
            markup.add(InlineKeyboardButton(button_text, callback_data=f"tag_view:{tag_info['id']}"))
        
        # Кнопка "✅ Просмотренные" если есть просмотренные теги
        if watched_tags:
            watched_count = len(watched_tags)
            watched_button_text = f"✅ Просмотренные ({watched_count})"
            markup.add(InlineKeyboardButton(watched_button_text, callback_data="watched_tags_list"))
        
        markup.add(InlineKeyboardButton("◀️ Назад в базу", callback_data="back_to_database"))
        
        bot.edit_message_text(text, chat_id, call.message.message_id, parse_mode='HTML', reply_markup=markup)
    except Exception as e:
        logger.error(f"[TAGS LIST] Ошибка: {e}", exc_info=True)


def show_database_menu(chat_id, user_id, message_id=None):
    """Показывает меню базы фильмов"""
    text = "🗄️ <b>Это ваша база фильмов.</b>\n\n"
    text += "Тут вы можете посмотреть список непросмотренных фильмов, поставить оценки или посмотреть добавленные подборки."
    
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("🗃️ Непросмотренные", callback_data="database:unwatched"))
    markup.add(InlineKeyboardButton("⚖️ Неоценённые", callback_data="database:unrated"))
    markup.add(InlineKeyboardButton("🏷️ Подборки", callback_data="database:tags"))
    markup.add(InlineKeyboardButton("◀️ Назад в меню", callback_data="back_to_start_menu"))
    
    try:
        if message_id:
            bot.edit_message_text(text, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
        else:
            bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup)
    except Exception as e:
        logger.error(f"[DATABASE MENU] Ошибка: {e}", exc_info=True)


@bot.callback_query_handler(func=lambda call: call.data.startswith("database:"))
def handle_database_action(call):
    """Обработчик действий в меню базы"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    action = call.data.split(":")[1]
    
    try:
        try:
            bot.answer_callback_query(call.id)
        except Exception as e:
            logger.warning(f"[DATABASE ACTION] Не удалось ответить на callback: {e}")
        
        if action == "unwatched":
            # Вызываем /list
            from moviebot.bot.handlers.list import show_list_page
            show_list_page(bot, chat_id, user_id, page=1, message_id=call.message.message_id)
        elif action == "unrated":
            # Вызываем /rate - используем внутреннюю функцию rate_movie из register_rate_handlers
            # Создаем фейковое сообщение для вызова обработчика
            class FakeMessage:
                def __init__(self, call):
                    self.from_user = call.from_user
                    self.chat = call.message.chat
                    self.text = '/rate'
                    self.message_id = call.message.message_id
            fake_msg = FakeMessage(call)
            # Импортируем и вызываем обработчик напрямую
            from moviebot.bot.handlers.rate import register_rate_handlers
            # Создаем временный обработчик для вызова
            import types
            temp_bot = types.SimpleNamespace()
            temp_bot.reply_to = lambda msg, text, **kwargs: bot.send_message(call.message.chat.id, text, **kwargs)
            # Вызываем обработчик напрямую
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                # Получаем все просмотренные фильмы без оценок
                with db_lock:
                    cursor_local.execute('''
                        SELECT m.id, m.kp_id, m.title, m.year
                        FROM movies m
                        WHERE m.chat_id = %s AND m.watched = 1
                        AND NOT (
                            NOT EXISTS (
                                SELECT 1 FROM ratings r 
                                WHERE r.chat_id = m.chat_id 
                                AND r.film_id = m.id 
                                AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                            )
                            AND EXISTS (
                                SELECT 1 FROM ratings r 
                                WHERE r.chat_id = m.chat_id 
                                AND r.film_id = m.id 
                                AND r.is_imported = TRUE
                            )
                        )
                        AND NOT EXISTS (
                            SELECT 1 FROM ratings r 
                            WHERE r.chat_id = m.chat_id 
                            AND r.film_id = m.id 
                            AND r.user_id = %s
                            AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                        )
                        ORDER BY m.title
                        LIMIT 10
                    ''', (chat_id, user_id))
                    unwatched_films = cursor_local.fetchall()
            except Exception as db_e:
                logger.error(f"[DATABASE ACTION] Ошибка запроса списка фильмов: {db_e}", exc_info=True)
                try:
                    bot.edit_message_text("❌ Ошибка доступа к базе данных", chat_id, call.message.message_id)
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
            
            if not unwatched_films:
                text = "✅ Все просмотренные фильмы уже оценены!\n\nВы можете:\n• Отметить фильм просмотренным в базе\n• Найти фильм, который вы смотрели, через поиск"
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("🗃️ Перейти в базу", callback_data="database:unwatched"))
                markup.add(InlineKeyboardButton("🔍 Найти фильм", callback_data="start_menu:search"))
                markup.add(InlineKeyboardButton("◀️ Назад в базу", callback_data="back_to_database"))
                try:
                    bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except:
                    bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
                return
            
            # Формируем список фильмов для оценки
            text = "⭐ <b>Оцените просмотренные фильмы:</b>\n\n"
            markup = InlineKeyboardMarkup(row_width=1)
            
            for row in unwatched_films:
                if isinstance(row, dict):
                    film_id = row.get('id')
                    kp_id = row.get('kp_id')
                    title = row.get('title')
                    year = row.get('year')
                else:
                    film_id = row[0] if row else None
                    kp_id = row[1]
                    title = row[2]
                    year = row[3] if len(row) > 3 else '—'
                
                text += f"• <b>{title}</b> ({year})\n"
                button_text = f"{title} ({year})"
                if len(button_text) > 50:
                    button_text = button_text[:47] + "..."
                markup.add(InlineKeyboardButton(button_text, callback_data=f"rate_from_list:{int(kp_id)}"))
            
            text += "\n<i>Нажмите на фильм, чтобы открыть его описание и оценить</i>"
            markup.add(InlineKeyboardButton("◀️ Назад в базу", callback_data="back_to_database"))
            
            try:
                bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            except:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        elif action == "tags":
            # Показываем список тегов
            tags_command(call.message)
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except:
                pass
    except Exception as e:
        logger.error(f"[DATABASE ACTION] Ошибка: {e}", exc_info=True)


@bot.callback_query_handler(func=lambda call: call.data == "back_to_database")
def handle_back_to_database(call):
    """Обработчик возврата в меню базы"""
    try:
        bot.answer_callback_query(call.id)
        show_database_menu(call.message.chat.id, call.from_user.id, call.message.message_id)
    except Exception as e:
        logger.error(f"[BACK TO DATABASE] Ошибка: {e}", exc_info=True)


@bot.callback_query_handler(func=lambda call: call.data.startswith("view_film_from_tag:"))
def view_film_from_tag_callback(call):
    """Обработчик кнопки 'Перейти к описанию' из подборки"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    tag_id = int(call.data.split(":")[1])
    
    callback_is_old = False
    try:
        bot.answer_callback_query(call.id, "Пришлите ссылку или ID фильма")
    except Exception as answer_error:
        error_str = str(answer_error)
        if "query is too old" in error_str or "query ID is invalid" in error_str or "timeout expired" in error_str:
            callback_is_old = True
            logger.warning(f"[VIEW FILM FROM TAG] Callback query устарел: {answer_error}")
        else:
            logger.error(f"[VIEW FILM FROM TAG] Ошибка answer_callback_query: {answer_error}", exc_info=True)
    
    if callback_is_old:
        return
    
    try:
        logger.info(f"[VIEW FILM FROM TAG] user_id={user_id}, tag_id={tag_id}")
        user_view_film_state[user_id] = {
            'chat_id': chat_id,
            'tag_id': tag_id  # Сохраняем tag_id для возврата
        }
        prompt_msg = bot.send_message(chat_id, "Пришлите в ответном сообщении ссылку или ID фильма, чье описание хотите посмотреть")
        user_view_film_state[user_id]['prompt_message_id'] = prompt_msg.message_id
    except Exception as e:
        logger.error(f"[VIEW FILM FROM TAG] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("plan_from_tag:"))
def plan_from_tag_callback(call):
    """Обработчик кнопки 'Запланировать просмотр' из подборки"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    tag_id = int(call.data.split(":")[1])
    
    callback_is_old = False
    try:
        bot.answer_callback_query(call.id, "Пришлите ссылку или ID фильма")
    except Exception as answer_error:
        error_str = str(answer_error)
        if "query is too old" in error_str or "query ID is invalid" in error_str or "timeout expired" in error_str:
            callback_is_old = True
            logger.warning(f"[PLAN FROM TAG] Callback query устарел: {answer_error}")
        else:
            logger.error(f"[PLAN FROM TAG] Ошибка answer_callback_query: {answer_error}", exc_info=True)
    
    if callback_is_old:
        return
    
    try:
        logger.info(f"[PLAN FROM TAG] user_id={user_id}, tag_id={tag_id}")
        user_plan_state[user_id] = {
            'step': 1,
            'chat_id': chat_id,
            'tag_id': tag_id  # Сохраняем tag_id для возврата
        }
        prompt_msg = bot.send_message(chat_id, "Пришлите ссылку или ID фильма в ответном сообщении и напишите, где (дома или в кино) и когда вы хотели бы его посмотреть!")
        user_plan_state[user_id]['prompt_message_id'] = prompt_msg.message_id
    except Exception as e:
        logger.error(f"[PLAN FROM TAG] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("mark_watched_from_tag:"))
def mark_watched_from_tag_callback(call):
    """Обработчик кнопки 'Отметить просмотренным' из подборки"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    tag_id = int(call.data.split(":")[1])
    
    callback_is_old = False
    try:
        bot.answer_callback_query(call.id)
    except Exception as answer_error:
        error_str = str(answer_error)
        if "query is too old" in error_str or "query ID is invalid" in error_str or "timeout expired" in error_str:
            callback_is_old = True
            logger.warning(f"[MARK WATCHED FROM TAG] Callback query устарел: {answer_error}")
        else:
            logger.error(f"[MARK WATCHED FROM TAG] Ошибка answer_callback_query: {answer_error}", exc_info=True)
    
    if callback_is_old:
        return
    
    try:
        logger.info(f"[MARK WATCHED FROM TAG] user_id={user_id}, tag_id={tag_id}")
        user_mark_watched_state[user_id] = {
            'chat_id': chat_id,
            'tag_id': tag_id  # Сохраняем tag_id для возврата
        }
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("◀️ Назад к подборке", callback_data=f"back_to_tag:{tag_id}"))
        prompt_msg = bot.send_message(
            chat_id,
            "👁️ Отметить просмотренным\n\nПришлите ID фильма из списка или ссылку на фильм, который хотите отметить просмотренным. Фильм автоматически отметится просмотренным.",
            reply_markup=markup
        )
        user_mark_watched_state[user_id]['prompt_message_id'] = prompt_msg.message_id
    except Exception as e:
        logger.error(f"[MARK WATCHED FROM TAG] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("tag_add_to_group:"))
def handle_tag_add_to_group(call):
    """Обработчик кнопки 'Добавить в группу'"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    tag_id = int(call.data.split(":")[1])
    
    try:
        bot.answer_callback_query(call.id)
        
        # Получаем список общих групп
        common_groups = []
        conn_groups = get_db_connection()
        cursor_groups = get_db_cursor()
        try:
            with db_lock:
                cursor_groups.execute('''
                    SELECT DISTINCT chat_id 
                    FROM subscriptions 
                    WHERE user_id = %s AND chat_id < 0
                ''', (user_id,))
                user_groups = [row[0] if isinstance(row, tuple) else row.get('chat_id') for row in cursor_groups.fetchall()]
                
                for group_id in user_groups:
                    try:
                        chat = bot.get_chat(group_id)
                        if chat.type in ['group', 'supergroup']:
                            try:
                                member = bot.get_chat_member(group_id, bot.get_me().id)
                                if member.status in ['member', 'administrator', 'creator']:
                                    common_groups.append((group_id, chat.title or f"Группа {group_id}"))
                            except:
                                pass
                    except Exception as e:
                        logger.warning(f"[TAG ADD TO GROUP] Ошибка проверки группы {group_id}: {e}")
                        continue
        except Exception as e:
            logger.error(f"[TAG ADD TO GROUP] Ошибка получения списка групп: {e}", exc_info=True)
        finally:
            try:
                cursor_groups.close()
            except:
                pass
            try:
                conn_groups.close()
            except:
                pass
        
        if not common_groups:
            bot.answer_callback_query(call.id, "❌ Не найдено общих групп", show_alert=True)
            return
        
        # Показываем список групп для выбора
        text = "📢 <b>Выберите группу для добавления подборки:</b>\n\n"
        markup = InlineKeyboardMarkup(row_width=1)
        
        for group_id, group_title in common_groups:
            # Ограничиваем длину названия
            button_text = group_title[:50] if len(group_title) <= 50 else group_title[:47] + "..."
            markup.add(InlineKeyboardButton(button_text, callback_data=f"tag_select_group:{tag_id}:{group_id}"))
        
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="tag_cancel_group"))
        
        bot.edit_message_text(text, chat_id, call.message.message_id, parse_mode='HTML', reply_markup=markup)
        
    except Exception as e:
        logger.error(f"[TAG ADD TO GROUP] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("tag_select_group:"))
def handle_tag_select_group(call):
    """Обработчик выбора группы для добавления подборки"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    parts = call.data.split(":")
    tag_id = int(parts[1])
    target_group_id = int(parts[2])
    
    try:
        bot.answer_callback_query(call.id, "⏳ Добавляю подборку в группу...")
        
        # Получаем информацию о теге
        conn = get_db_connection()
        cursor = get_db_cursor()
        tag_info = None
        tag_movies = []
        
        try:
            with db_lock:
                cursor.execute('SELECT id, name, short_code FROM tags WHERE id = %s', (tag_id,))
                row = cursor.fetchone()
                if not row:
                    bot.answer_callback_query(call.id, "❌ Подборка не найдена", show_alert=True)
                    return
                
                tag_info = {
                    'id': row[0] if isinstance(row, tuple) else row.get('id'),
                    'name': row[1] if isinstance(row, tuple) else row.get('name'),
                    'short_code': row[2] if isinstance(row, tuple) else row.get('short_code')
                }
                
                # Получаем все фильмы из подборки
                cursor.execute('SELECT kp_id, is_series FROM tag_movies WHERE tag_id = %s', (tag_id,))
                tag_movies = cursor.fetchall()
        except Exception as e:
            logger.error(f"[TAG SELECT GROUP] Ошибка получения информации о теге: {e}", exc_info=True)
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
            return
        finally:
            try:
                cursor.close()
            except:
                pass
            try:
                conn.close()
            except:
                pass
        
        if not tag_info or not tag_movies:
            bot.answer_callback_query(call.id, "❌ Подборка пуста", show_alert=True)
            return
        
        # Отправляем сообщение в группу с deep link
        bot_username = bot.get_me().username
        deep_link = f"https://t.me/{bot_username}?start=tag_{tag_info['short_code']}"
        
        group_text = f"📦 <b>Подборка: {tag_info['name']}</b>\n\n"
        group_text += f"🎬 Фильмов/сериалов в подборке: {len(tag_movies)}\n\n"
        group_text += f"🔗 Добавить подборку в базу:\n"
        group_text += f"<code>{deep_link}</code>"
        
        try:
            bot.send_message(target_group_id, group_text, parse_mode='HTML')
            bot.edit_message_text(
                f"✅ Подборка <b>\"{tag_info['name']}\"</b> отправлена в группу!",
                chat_id, call.message.message_id, parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"[TAG SELECT GROUP] Ошибка отправки в группу: {e}", exc_info=True)
            bot.answer_callback_query(call.id, "❌ Не удалось отправить в группу", show_alert=True)
            
    except Exception as e:
        logger.error(f"[TAG SELECT GROUP] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data == "tag_cancel_group")
def handle_tag_cancel_group(call):
    """Обработчик отмены выбора группы"""
    try:
        bot.answer_callback_query(call.id)
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except:
        pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("back_to_tag:"))
def handle_back_to_tag(call):
    """Обработчик возврата к подборке"""
    user_id = call.from_user.id
    tag_id = int(call.data.split(":")[1])
    
    try:
        bot.answer_callback_query(call.id)
        
        # Очищаем состояние
        if user_id in user_mark_watched_state:
            del user_mark_watched_state[user_id]
        
        # Получаем текущую страницу из состояния
        state = user_tag_list_state.get(user_id)
        if state and state.get('tag_id') == tag_id:
            page = state.get('page', 1)
            show_tag_films_page(bot, call.message.chat.id, user_id, tag_id, page, call.message.message_id)
        else:
            show_tag_films_page(bot, call.message.chat.id, user_id, tag_id, 1, call.message.message_id)
    except Exception as e:
        logger.error(f"[BACK TO TAG] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except:
            pass
