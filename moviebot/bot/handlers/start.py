from moviebot.bot.bot_init import bot
"""
Обработчики команды /start и главного меню
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from moviebot.database.db_operations import (
    get_active_subscription,
    get_active_group_subscription_by_chat_id,
    get_user_personal_subscriptions,
    log_request
)
from moviebot.utils.helpers import has_recommendations_access
from moviebot.states import user_plan_state

from moviebot.bot.bot_init import safe_answer_callback_query

logger = logging.getLogger(__name__)

logger.info("[START.PY] Модуль start.py загружен")

# Регистрируем команду /code на уровне модуля, чтобы она точно работала
@bot.message_handler(commands=['code'])
def handle_code_command(message):
    """Команда /code - генерация кода для привязки браузерного расширения"""
    logger.info(f"[CODE] ===== START: user_id={message.from_user.id}, chat_id={message.chat.id}")
    import secrets
    from datetime import datetime, timedelta
    from moviebot.database.db_connection import get_db_connection, get_db_cursor
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    code = secrets.token_hex(5).upper()  # 10 символов
    expires = datetime.utcnow() + timedelta(minutes=10)
    
    conn = get_db_connection()
    cursor = get_db_cursor()
    try:
        # Без db_lock как просил пользователь
        cursor.execute("""
            INSERT INTO extension_links (code, user_id, chat_id, expires_at, used)
            VALUES (%s, %s, %s, %s, FALSE)
            ON CONFLICT (code) DO UPDATE SET 
                user_id = EXCLUDED.user_id,
                chat_id = EXCLUDED.chat_id,
                expires_at = EXCLUDED.expires_at,
                used = FALSE
        """, (code, user_id, chat_id, expires))
        conn.commit()
        
        logger.info(f"[CODE] Код сгенерирован: {code} для user_id={user_id}, chat_id={chat_id}")
        bot.reply_to(message,
            f"Код для входа: <code>{code}</code>\n\n"
            "Используйте его для входа в <a href=\"https://movie-planner.ru\">личном кабинете</a> или в расширении. Действует 10 минут.",
            parse_mode='HTML')
    except Exception as e:
        logger.error(f"[CODE] Ошибка генерации кода: {e}", exc_info=True)
        bot.reply_to(message, "❌ Не получилось сгенерировать код. Попробуй позже.")
    finally:
        try:
            cursor.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass
    logger.info(f"[CODE] ===== END =====")

logger.info("[START.PY] Команда /code зарегистрирована на уровне модуля")

# Регистрируем callback handlers на уровне модуля (до функции register_start_handlers)
@bot.callback_query_handler(func=lambda call: call.data.startswith("start_menu:"))
def start_menu_callback(call):
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        message_thread_id = getattr(call.message, 'message_thread_id', None)
        
        # Парсим callback_data: start_menu:action или start_menu:action:winner_id
        parts = call.data.split(":")
        action = parts[1]
        expected_user_id = None
        if len(parts) > 2:
            try:
                expected_user_id = int(parts[2])
            except (ValueError, IndexError):
                pass
        
        # Проверяем, что кнопка доступна только для победителя/участника (ДО ответа на callback)
        if expected_user_id is not None and user_id != expected_user_id:
            try:
                bot.answer_callback_query(call.id, "Эта кнопка доступна только для победителя случайного события", show_alert=True)
                logger.info(f"[START MENU] Показана ошибка пользователю {user_id} (кнопка для {expected_user_id})")
            except Exception as e:
                logger.warning(f"[START MENU] Не удалось показать ошибку: {e}")
            logger.info(f"[START MENU] Пользователь {user_id} пытается использовать кнопку, предназначенную для {expected_user_id}")
            return
        
        # Отвечаем на callback только если проверка прошла
        safe_answer_callback_query(bot, call.id)

        logger.info(f"[START MENU] Обработка действия: {action}, user_id={user_id}")

        # Импортируем нужные функции один раз здесь
        from moviebot.bot.handlers.plan import show_schedule
        from moviebot.bot.handlers.payment import payment_command
        from moviebot.bot.handlers.series import handle_search, random_start, premieres_command, ticket_command, help_command, HELP_INTRO_TEXT
        from moviebot.bot.handlers.seasons import show_seasons_list

        # Обычный импорт settings_main
        from moviebot.bot.handlers.settings_main import settings_command

        # Обработка locked билетов (только в группах; в личке билеты открыты для всех)
        if action == 'tickets_locked':
            text = "🎫 <b>Билеты в кино</b>\n\nВ групповых чатах загрузка билетов доступна с подпиской <b>💎 Movie Planner PRO</b>.\n\nИспользуйте /payment для оформления подписки."
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("💎 Movie Planner PRO", callback_data="payment:tariffs:personal"))
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            try:
                bot.edit_message_text(
                    text=text,
                    chat_id=chat_id,
                    message_id=message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
            except Exception as e:
                logger.warning(f"[START MENU] Не удалось отредактировать: {e}")
                bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    reply_markup=markup,
                    parse_mode='HTML',
                    message_thread_id=message_thread_id
                )
            return
        
        # Обработка билетов - показываем список событий
        if action == 'tickets':
            from moviebot.bot.handlers.series import show_cinema_sessions
            show_cinema_sessions(chat_id, user_id, None)
            return

        if action == 'seasons':
            bot.answer_callback_query(call.id, "⏳ Загружаем сериалы и сезоны...")
            # show_seasons_list редактирует существующее сообщение, не удаляем его
            show_seasons_list(
                chat_id=chat_id,
                user_id=user_id,
                message_id=message_id,
                message_thread_id=message_thread_id,
                bot=bot
            )
            return  # Не удаляем сообщение, так как оно редактируется
        elif action == 'premieres':
            msg = call.message
            msg.text = '/premieres'
            premieres_command(msg)

        elif action == 'what_to_watch':
            # Все режимы отображаем без замка (доступ при выборе: подписка или 3 бесплатных использования)
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("🎲 Рандом по своей базе", callback_data="rand_mode:database"))
            markup.add(InlineKeyboardButton("⭐ По оценкам в базе", callback_data="rand_mode:group_votes"))
            markup.add(InlineKeyboardButton("🎬 Рандом по кинопоиску", callback_data="rand_mode:kinopoisk"))
            markup.add(InlineKeyboardButton("⭐ По моим оценкам (9-10)", callback_data="rand_mode:my_votes"))
            markup.add(InlineKeyboardButton("🔮 Шазам", callback_data="shazam:start"))
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            try:
                bot.edit_message_text(
                    "🤔 <b>Что посмотреть?</b>\n\nВыберите режим:",
                    chat_id, message_id, reply_markup=markup, parse_mode='HTML'
                )
            except Exception as e:
                logger.warning(f"[START MENU] edit what_to_watch: {e}")
                bot.send_message(chat_id, "🤔 <b>Что посмотреть?</b>\n\nВыберите режим:", reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
            return

        elif action == 'random':
            # Оставлено для обратной совместимости; основной вход — через «Что посмотреть?»
            msg = call.message
            msg.from_user = call.from_user
            msg.text = '/random'
            random_start(msg)

        elif action == 'search':
            msg = call.message
            msg.text = '/search'
            handle_search(msg)

        elif action == 'schedule':
            msg = call.message
            msg.text = '/schedule'
            show_schedule(msg)

        elif action == 'tickets':
            from moviebot.bot.handlers.series import show_cinema_sessions
            show_cinema_sessions(chat_id, user_id, None)
            return

        elif action == 'payment':
            msg = call.message
            msg.text = '/payment'
            payment_command(msg)

        elif action == 'settings':
            msg = call.message
            msg.text = '/settings'
            msg.from_user = call.from_user  # пользователь, нажавший кнопку (сообщение от бота — from_user был бы бот)
            settings_command(msg)

        elif action == 'help':
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("🎬 Помощь по использованию бота", callback_data="help:bot_usage"))
            markup.add(InlineKeyboardButton("📖 Сценарии взаимодействия с сервисом", callback_data="help:scenarios"))
            markup.add(InlineKeyboardButton("💻 Работа с расширением", callback_data="help:extension"))
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            try:
                bot.edit_message_text(
                    text=HELP_INTRO_TEXT,
                    chat_id=chat_id,
                    message_id=message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
            except Exception as e:
                logger.warning(f"[START MENU] edit help intro: {e}")
                bot.send_message(chat_id, HELP_INTRO_TEXT, reply_markup=markup, parse_mode='HTML')
            return
        
        elif action == 'database':
            # Показываем меню базы
            from moviebot.bot.handlers.tags import show_database_menu
            show_database_menu(call.message.chat.id, user_id, call.message.message_id)
            return

        elif action == 'extension':
            # Показываем информацию о браузерном расширении
            text = (
                "💻 <b>Браузерное расширение Movie Planner Bot</b>\n\n"
                "Расширение решает три задачи:\n"
                "1️⃣ Добавление в базу и планирование фильмов (Кинопоиск, IMDb, Letterboxd)\n"
                "2️⃣ Помощь в сохранении билетов в кино при покупке из браузера\n"
                "3️⃣ Трекинг сериалов на стримингах (Амедиатека, Okko, ivi, hd.kinopoisk, tvoe, Start, Premier, Wink и др.)\n\n"
                "🔗 <b>Установить расширение:</b>\n"
                "https://chromewebstore.google.com/detail/movie-planner-bot/fldeclcfcngcjphhklommcebkpfipdol\n\n"
                "Для подключения расширения к вашей базе потребуется ввести код. Вы можете подключить расширение к личной базе или групповой. "
                "Код действителен в течение 10 минут. Возможности расширения открываются согласно вашему тарифу, полный функционал доступен с пакетным тарифом.\n\n"
                "Нажмите, чтобы получить код ⬇️"
            )
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔢 Получить код", callback_data="extension:get_code"))
            markup.add(InlineKeyboardButton("◀️ Назад в меню", callback_data="back_to_start_menu"))
            try:
                bot.edit_message_text(
                    text=text,
                    chat_id=chat_id,
                    message_id=message_id,
                    reply_markup=markup,
                    parse_mode='HTML',
                    disable_web_page_preview=True
                )
            except Exception as e:
                logger.warning(f"[START MENU] Не удалось отредактировать для extension: {e}")
                bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    reply_markup=markup,
                    parse_mode='HTML',
                    message_thread_id=message_thread_id,
                    disable_web_page_preview=True
                )
            return

        # Удаляем старое меню для всех действий
        try:
            bot.delete_message(chat_id, message_id)
        except:
            pass

    except Exception as e:
        logger.error(f"[START MENU] Ошибка в обработчике: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Произошла ошибка", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data == "extension:get_code")
def extension_get_code_callback(call):
    """Генерация кода для привязки браузерного расширения через callback"""
    try:
        import secrets
        from datetime import datetime, timedelta
        from moviebot.database.db_connection import get_db_connection, get_db_cursor
        
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        
        safe_answer_callback_query(bot, call.id, "⏳ Генерируем код...")
        
        code = secrets.token_hex(5).upper()  # 10 символов
        expires = datetime.utcnow() + timedelta(minutes=10)
        
        conn = get_db_connection()
        cursor = get_db_cursor()
        try:
            cursor.execute("""
                INSERT INTO extension_links (code, user_id, chat_id, expires_at, used)
                VALUES (%s, %s, %s, %s, FALSE)
                ON CONFLICT (code) DO UPDATE SET 
                    user_id = EXCLUDED.user_id,
                    chat_id = EXCLUDED.chat_id,
                    expires_at = EXCLUDED.expires_at,
                    used = FALSE
            """, (code, user_id, chat_id, expires))
            conn.commit()
            
            logger.info(f"[EXTENSION CODE] Код сгенерирован: {code} для user_id={user_id}, chat_id={chat_id}")
            
            text = (
                f"🔢 <b>Код для входа:</b>\n\n"
                f"<code>{code}</code>\n\n"
                f"Используйте его для входа в <a href=\"https://movie-planner.ru\">личном кабинете</a> или в расширении.\n"
                f"⏰ Код действует 10 минут."
            )
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("◀️ Назад в меню", callback_data="back_to_start_menu"))
            
            bot.edit_message_text(
                text=text,
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"[EXTENSION CODE] Ошибка генерации кода: {e}", exc_info=True)
            bot.answer_callback_query(call.id, "❌ Не удалось сгенерировать код", show_alert=True)
        finally:
            try:
                cursor.close()
            except:
                pass
            try:
                conn.close()
            except:
                pass
    except Exception as e:
        logger.error(f"[EXTENSION CODE] Общая ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Произошла ошибка", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data == "back_to_start_menu")
def back_to_start_menu_callback(call):
    try:
        bot.answer_callback_query(call.id, "⏳ Возвращаемся...")

        user_id = call.from_user.id
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        
        # Очищаем состояние планирования, если оно есть
        if user_id in user_plan_state:
            del user_plan_state[user_id]
            logger.info(f"[BACK TO MENU] Очищено состояние планирования для user_id={user_id}")

        # Та же логика подписки, что и в /start (теперь с группой)
        subscription_info = ""
        try:
            if call.message.chat.type == 'private':
                sub = get_active_subscription(chat_id, user_id, 'personal')
                if sub:
                    plan_type = sub.get('plan_type', 'all')
                    plan_name = "💎 Movie Planner PRO" if plan_type == 'all' else plan_type
                    subscription_info = f"\n\n<b>Ваша подписка:</b> {plan_name}\n"
                else:
                    subscription_info = "\n\n<b>Базовая версия бота</b>\n"
            else:
                group_sub = get_active_group_subscription_by_chat_id(chat_id)
                if group_sub:
                    plan_type = group_sub.get('plan_type', 'all')
                    plan_name = "💎 Movie Planner PRO" if plan_type == 'all' else plan_type
                    subscription_info = f"\n\n<b>Подписка группы:</b> {plan_name}\n"
                else:
                    subscription_info = "\n\n<b>Базовая версия бота</b>\n"
        except Exception as sub_error:
            logger.error(f"[BACK TO MENU] Ошибка получения информации о подписке: {sub_error}", exc_info=True)
            subscription_info = "\n\n<b>Базовая версия бота</b>\n"

        welcome_text = f"""
🎬 <b>Главное меню</b>{subscription_info}

💌 Чтобы добавить в базу фильм или сериал, пришлите в сообщении ссылку на страницу фильма или сериала на Кинопоиске в бот.

Выберите раздел из меню ниже ⬇
        """.strip()

        markup = InlineKeyboardMarkup()

        # Строка 1: Сериалы / Премьеры
        markup.row(
            InlineKeyboardButton("📺 Сериалы", callback_data="start_menu:seasons"),
            InlineKeyboardButton("📅 Премьеры", callback_data="start_menu:premieres")
        )
        # Строка 2: только Поиск
        markup.row(InlineKeyboardButton("🔍 Поиск", callback_data="start_menu:search"))
        # Строка 3: База (слева) / Расписание (справа)
        markup.row(
            InlineKeyboardButton("🗄️ База", callback_data="start_menu:database"),
            InlineKeyboardButton("🗓️ Расписание", callback_data="start_menu:schedule")
        )
        # Строка 4: Что посмотреть? (слева) / Билеты (справа); в личке билеты для всех
        tickets_text = "🎫 Билеты"
        tickets_callback = "start_menu:tickets"
        markup.row(
            InlineKeyboardButton("🤔 Что посмотреть?", callback_data="start_menu:what_to_watch"),
            InlineKeyboardButton(tickets_text, callback_data=tickets_callback)
        )
        # Строка 5: Оплата / Расширение / Настройки / Помощь
        markup.row(
            InlineKeyboardButton("💰", callback_data="start_menu:payment"),
            InlineKeyboardButton("💻", callback_data="start_menu:extension"),
            InlineKeyboardButton("⚙️", callback_data="start_menu:settings"),
            InlineKeyboardButton("❓", callback_data="start_menu:help")
        )

        bot.edit_message_text(
            text=welcome_text,
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=markup,
            parse_mode='HTML'
        )

    except Exception as e:
        logger.error(f"[BACK TO MENU] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка возврата в меню", show_alert=True)
        except:
            pass

logger.info("[START.PY] Callback handlers для start_menu и back_to_start_menu зарегистрированы на уровне модуля")

def register_start_handlers(bot):
    """Регистрация всех обработчиков из этого модуля"""
    logger.info("[REGISTER START HANDLERS] ===== НАЧАЛО РЕГИСТРАЦИИ =====")

    @bot.message_handler(commands=['start', 'menu'])
    def send_welcome(message):
        logger.info(f"[START HANDLER] ===== СРАБОТАЛ /start от user_id={message.from_user.id}, chat_id={message.chat.id} =====")
        
        # Логируем полный текст сообщения для отладки
        message_text = message.text or ""
        logger.info(f"[START] Полный текст сообщения: '{message_text}', entities: {getattr(message, 'entities', None)}")

        try:
            command_type = '/start' if message_text.startswith('/start') else '/menu'
            logger.info(f"[HANDLER] {command_type} вызван от {message.from_user.id}, chat_type={message.chat.type}")
            username = message.from_user.username or f"user_{message.from_user.id}"
            log_request(message.from_user.id, username, '/start', message.chat.id)
        except Exception as e:
            logger.error(f"[SEND_WELCOME] Ошибка в начале функции: {e}", exc_info=True)
            try:
                bot.reply_to(message, "❌ Произошла ошибка. Попробуйте еще раз.")
            except:
                pass
            return

        # Определяем переменные для удобства и фикса NameError
        chat_id = message.chat.id
        user_id = message.from_user.id
        
        # Проверяем, есть ли параметр start_parameter (для deep links)
        start_param = None
        if message_text.startswith('/start'):
            parts = message_text.split(' ', 1)
            if len(parts) > 1:
                start_param = parts[1].strip()
                logger.info(f"[START] Обнаружен start_parameter: {start_param}")
        
        # Deep link: ?start=code — сразу отправить код (как /code), без главного меню
        if start_param and start_param.strip().lower() == 'code':
            logger.info(f"[START CODE] Отправка кода по deep link для user_id={user_id}, chat_id={chat_id}")
            try:
                handle_code_command(message)
            except Exception as e:
                logger.error(f"[START CODE] Ошибка: {e}", exc_info=True)
                try:
                    bot.reply_to(message, "❌ Не удалось сгенерировать код. Напиши /code в чат.")
                except Exception:
                    pass
            return
        
        # Deep link: ?start=g{group_chat_id}_{film_id} — открыть карточку фильма в группе
        if start_param and start_param.startswith('g') and '_' in start_param:
            try:
                rest = start_param[1:].strip()
                parts = rest.split('_', 1)
                if len(parts) == 2 and parts[0].lstrip('-').isdigit() and parts[1].isdigit():
                    group_chat_id = int(parts[0])
                    film_id = int(parts[1])
                    from moviebot.bot.handlers.series import show_film_info_with_buttons
                    from moviebot.api.kinopoisk_api import extract_movie_info
                    from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
                    try:
                        bot.get_chat_member(group_chat_id, user_id)
                    except Exception:
                        bot.reply_to(message, "❌ Вас нет в этой группе или бот не добавлен туда.")
                        return
                    conn = get_db_connection()
                    cur = get_db_cursor()
                    with db_lock:
                        cur.execute(
                            "SELECT id, kp_id, link, is_series, title, watched FROM movies WHERE chat_id = %s AND id = %s",
                            (group_chat_id, film_id)
                        )
                        row = cur.fetchone()
                    if not row:
                        bot.reply_to(message, "❌ Фильм не найден в этой группе.")
                        return
                    fid = row.get('id') if isinstance(row, dict) else row[0]
                    kp_id_str = str(row.get('kp_id') if isinstance(row, dict) else row[1])
                    link = (row.get('link') or '').strip() if isinstance(row, dict) else (row[2] or '').strip()
                    is_series = bool(row.get('is_series') if isinstance(row, dict) else (row[3] if len(row) > 3 else False))
                    title_db = row.get('title') if isinstance(row, dict) else row[4]
                    watched = bool(row.get('watched') if isinstance(row, dict) else (row[5] if len(row) > 5 else False))
                    if not link:
                        link = f"https://www.kinopoisk.ru/series/{kp_id_str}/" if is_series else f"https://www.kinopoisk.ru/film/{kp_id_str}/"
                    info = extract_movie_info(link)
                    if info:
                        info['is_series'] = is_series
                        existing = (fid, title_db, watched)
                        show_film_info_with_buttons(group_chat_id, user_id, info, link, int(kp_id_str), existing=existing, message_id=None, message_thread_id=None)
                        try:
                            bot.reply_to(message, "✅ Карточка открыта в группе.")
                        except Exception:
                            pass
                    else:
                        bot.reply_to(message, "❌ Не удалось загрузить описание фильма.")
                else:
                    bot.reply_to(message, "Неверная ссылка.")
            except Exception as e:
                logger.error(f"[START G GROUP] Ошибка: {e}", exc_info=True)
                try:
                    bot.reply_to(message, "❌ Ошибка при открытии фильма в группе.")
                except Exception:
                    pass
            return

        # Deep link: ?start=view_film_{film_id или kp_id} — открыть карточку в личном чате
        if start_param and start_param.startswith('view_film_'):
            try:
                value_str = start_param.replace('view_film_', '').strip()
                if not value_str.isdigit():
                    bot.reply_to(message, "Неверная ссылка на фильм.")
                    return
                value_int = int(value_str)
                from moviebot.bot.handlers.series import show_film_info_with_buttons
                from moviebot.api.kinopoisk_api import extract_movie_info
                from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
                conn = get_db_connection()
                cur = get_db_cursor()
                existing = None
                link = None
                kp_id_int = None
                is_series = False
                with db_lock:
                    cur.execute(
                        "SELECT id, kp_id, link, is_series, title, watched FROM movies WHERE chat_id = %s AND id = %s",
                        (chat_id, value_int)
                    )
                    row = cur.fetchone()
                if row:
                    fid = row.get('id') if isinstance(row, dict) else row[0]
                    kp_id_int = int(row.get('kp_id') if isinstance(row, dict) else row[1])
                    link = (row.get('link') or '').strip() if isinstance(row, dict) else (row[2] or '').strip()
                    is_series = bool(row.get('is_series') if isinstance(row, dict) else (row[3] if len(row) > 3 else False))
                    title_db = row.get('title') if isinstance(row, dict) else row[4]
                    watched = bool(row.get('watched') if isinstance(row, dict) else row[5])
                    existing = (fid, title_db, watched)
                    if not link:
                        link = f"https://www.kinopoisk.ru/series/{kp_id_int}/" if is_series else f"https://www.kinopoisk.ru/film/{kp_id_int}/"
                else:
                    with db_lock:
                        cur.execute(
                            "SELECT id, kp_id, link, is_series, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s",
                            (chat_id, value_str)
                        )
                        row = cur.fetchone()
                    if row:
                        fid = row.get('id') if isinstance(row, dict) else row[0]
                        kp_id_int = int(row.get('kp_id') if isinstance(row, dict) else row[1])
                        link = (row.get('link') or '').strip() if isinstance(row, dict) else (row[2] or '').strip()
                        is_series = bool(row.get('is_series') if isinstance(row, dict) else (row[3] if len(row) > 3 else False))
                        title_db = row.get('title') if isinstance(row, dict) else row[4]
                        watched = bool(row.get('watched') if isinstance(row, dict) else row[5])
                        existing = (fid, title_db, watched)
                        if not link:
                            link = f"https://www.kinopoisk.ru/series/{kp_id_int}/" if is_series else f"https://www.kinopoisk.ru/film/{kp_id_int}/"
                    else:
                        kp_id_int = value_int
                        is_series = False
                        link = f"https://www.kinopoisk.ru/film/{kp_id_int}/"
                info = extract_movie_info(link)
                if info:
                    info['is_series'] = is_series
                    show_film_info_with_buttons(chat_id, user_id, info, link, kp_id_int, existing=existing, message_id=None, message_thread_id=None)
                else:
                    bot.reply_to(message, "❌ Не удалось загрузить описание фильма.")
            except Exception as e:
                logger.error(f"[START VIEW FILM] Ошибка: {e}", exc_info=True)
                try:
                    bot.reply_to(message, "❌ Ошибка при открытии фильма.")
                except Exception:
                    pass
            return
        
        # Deep link: ?start=search — открыть поиск
        if start_param and start_param.strip().lower() == 'search':
            try:
                from moviebot.bot.handlers.series import handle_search
                setattr(message, 'text', '/search')
                handle_search(message)
            except Exception as e:
                logger.error(f"[START SEARCH] Ошибка: {e}", exc_info=True)
            return
        # Deep link: ?start=premieres — открыть премьеры
        if start_param and start_param.strip().lower() == 'premieres':
            try:
                from moviebot.bot.handlers.series import premieres_command
                setattr(message, 'text', '/premieres')
                premieres_command(message)
            except Exception as e:
                logger.error(f"[START PREMIERES] Ошибка: {e}", exc_info=True)
            return
        # Deep link: ?start=random — открыть «Случайный фильм из базы»
        if start_param and start_param.strip().lower() == 'random':
            try:
                from moviebot.bot.handlers.series import random_start
                setattr(message, 'text', '/random')
                random_start(message)
            except Exception as e:
                logger.error(f"[START RANDOM] Ошибка: {e}", exc_info=True)
            return
        
        # Обработка deep link для тегов
        if start_param and start_param.startswith('tag_'):
            short_code = start_param.replace('tag_', '')
            logger.info(f"[START TAG] Обработка deep link для тега с кодом: {short_code}")
            try:
                from moviebot.bot.handlers.tags import handle_tag_deep_link, is_new_user
                
                # Проверяем, новый ли пользователь
                user_id = message.from_user.id
                chat_id = message.chat.id
                is_new = is_new_user(user_id, chat_id)
                
                if is_new:
                    # Если новый пользователь, сначала показываем приветствие, затем обрабатываем deep link
                    logger.info(f"[START TAG] Новый пользователь, показываем приветствие, затем deep link")
                    # Отправляем приветствие (send_welcome продолжит выполнение ниже)
                    # После отправки приветствия обработаем deep link
                else:
                    # Если не новый, сразу обрабатываем deep link и выходим
                    handle_tag_deep_link(bot, message, short_code)
                    return
            except Exception as e:
                logger.error(f"[START TAG] Ошибка обработки deep link: {e}", exc_info=True)
                # Продолжаем показ обычного меню при ошибке

        # Информация о подписке
        subscription_info = ""
        try:
            if message.chat.type == 'private':
                sub = get_active_subscription(chat_id, user_id, 'personal')
                if sub:
                    plan_type = sub.get('plan_type', 'all')
                    plan_name = "💎 Movie Planner PRO" if plan_type == 'all' else plan_type
                    subscription_info = f"\n\n<b>Ваша подписка:</b> {plan_name}\n"
                else:
                    subscription_info = "\n\n<b>Базовая версия бота</b>\n"
            else:
                group_sub = get_active_group_subscription_by_chat_id(chat_id)
                if group_sub:
                    plan_type = group_sub.get('plan_type', 'all')
                    plan_name = "💎 Movie Planner PRO" if plan_type == 'all' else plan_type
                    subscription_info = f"\n\n<b>Подписка группы:</b> {plan_name}\n"
                else:
                    subscription_info = "\n\n<b>Базовая версия бота</b>\n"
        except Exception as sub_error:
            logger.error(f"[START] Ошибка получения информации о подписке: {sub_error}", exc_info=True)
            subscription_info = "\n\n<b>Базовая версия бота</b>\n"

        welcome_text = f"""
🎬 <b>Главное меню</b>{subscription_info}

💌 Чтобы добавить в базу фильм или сериал, пришлите в сообщении ссылку на страницу фильма или сериала на Кинопоиске в бот, или воспользуйтесь кнопкой поиска ниже.

Выберите раздел из меню ниже ⬇
        """.strip()

        try:
            markup = InlineKeyboardMarkup()

            # Строка 1: Сериалы / Премьеры
            markup.row(
                InlineKeyboardButton("📺 Сериалы", callback_data="start_menu:seasons"),
                InlineKeyboardButton("📅 Премьеры", callback_data="start_menu:premieres")
            )
            markup.row(InlineKeyboardButton("🔍 Поиск", callback_data="start_menu:search"))
            markup.row(
                InlineKeyboardButton("🗄️ База", callback_data="start_menu:database"),
                InlineKeyboardButton("🗓️ Расписание", callback_data="start_menu:schedule")
            )
            tickets_text = "🎫 Билеты"
            tickets_callback = "start_menu:tickets"
            markup.row(
                InlineKeyboardButton("🤔 Что посмотреть?", callback_data="start_menu:what_to_watch"),
                InlineKeyboardButton(tickets_text, callback_data=tickets_callback)
            )
            markup.row(
                InlineKeyboardButton("💰", callback_data="start_menu:payment"),
                InlineKeyboardButton("💻", callback_data="start_menu:extension"),
                InlineKeyboardButton("⚙️", callback_data="start_menu:settings"),
                InlineKeyboardButton("❓", callback_data="start_menu:help")
            )

            try:
                # В группах пытаемся использовать reply_to для лучшей доставки
                if message.chat.type in ['group', 'supergroup']:
                    logger.info(f"[START] Попытка отправки в группе {chat_id} для пользователя {user_id}")
                    try:
                        sent_msg = bot.reply_to(message, welcome_text, parse_mode='HTML', reply_markup=markup)
                        if sent_msg:
                            logger.info(f"✅ Ответ на /start отправлен через reply_to пользователю {user_id} в группе {chat_id}, message_id={sent_msg.message_id}")
                        else:
                            logger.warning(f"[START] reply_to вернул None для пользователя {user_id} в группе {chat_id}")
                    except Exception as reply_error:
                        error_str = str(reply_error).lower()
                        logger.warning(f"[START] Не удалось отправить через reply_to: {reply_error} (тип: {type(reply_error).__name__})")
                        # Проверяем, не связана ли ошибка с правами бота
                        if "not enough rights" in error_str or "chat not found" in error_str or "bot was blocked" in error_str:
                            logger.error(f"[START] КРИТИЧЕСКАЯ ОШИБКА: {reply_error}")
                        try:
                            sent_msg = bot.send_message(
                                chat_id,
                                welcome_text,
                                parse_mode='HTML',
                                reply_markup=markup
                            )
                            if sent_msg:
                                logger.info(f"✅ Ответ на /start отправлен через send_message пользователю {user_id} в группе {chat_id}, message_id={sent_msg.message_id}")
                            else:
                                logger.warning(f"[START] send_message вернул None для пользователя {user_id} в группе {chat_id}")
                        except Exception as send_error2:
                            logger.error(f"[START] Не удалось отправить через send_message: {send_error2}", exc_info=True)
                            # Последняя попытка - простое сообщение
                            try:
                                bot.reply_to(message, "❌ Ошибка при загрузке меню. Попробуйте позже.")
                            except Exception as final_error:
                                logger.error(f"[START] Не удалось отправить даже простое сообщение: {final_error}")
                else:
                    # В личных чатах используем обычный send_message
                    try:
                        sent_msg = bot.send_message(
                            chat_id,
                            welcome_text,
                            parse_mode='HTML',
                            reply_markup=markup
                        )
                        if sent_msg:
                            logger.info(f"✅ Ответ на /start отправлен пользователю {user_id} в личном чате, message_id={sent_msg.message_id}")
                        else:
                            logger.error(f"❌ send_message вернул None для пользователя {user_id} в личном чате")
                    except Exception as send_msg_error:
                        logger.error(f"❌ Ошибка при send_message в личном чате: {send_msg_error}", exc_info=True)
            except Exception as send_error:
                logger.error(f"❌ Ошибка при отправке сообщения /start: {send_error}", exc_info=True)
                # Пробуем отправить простой ответ
                try:
                    bot.reply_to(message, "❌ Ошибка при загрузке меню. Попробуйте позже.")
                except:
                    pass
            
            # Если был deep link для тега и пользователь новый, обрабатываем его после приветствия
            if start_param and start_param.startswith('tag_'):
                short_code = start_param.replace('tag_', '')
                logger.info(f"[START TAG] Обработка deep link после приветствия для нового пользователя, code={short_code}")
                try:
                    from moviebot.bot.handlers.tags import handle_tag_deep_link
                    # Небольшая задержка, чтобы приветствие успело отправиться
                    import time
                    time.sleep(0.5)
                    handle_tag_deep_link(bot, message, short_code)
                except Exception as e:
                    logger.error(f"[START TAG] Ошибка обработки deep link после приветствия: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"❌ Ошибка при отправке меню: {e}", exc_info=True)
            try:
                bot.reply_to(message, "❌ Ошибка при загрузке меню. Попробуйте позже.")
            except:
                pass

    # Callback handlers для start_menu и back_to_start_menu уже зарегистрированы на уровне модуля выше
    # Не дублируем их здесь
            chat_id = call.message.chat.id
            message_id = call.message.message_id
            message_thread_id = getattr(call.message, 'message_thread_id', None)
            
            # Парсим callback_data: start_menu:action или start_menu:action:winner_id
            parts = call.data.split(":")
            action = parts[1]
            expected_user_id = None
            if len(parts) > 2:
                try:
                    expected_user_id = int(parts[2])
                except (ValueError, IndexError):
                    pass
            
            # Проверяем, что кнопка доступна только для победителя/участника (ДО ответа на callback)
            if expected_user_id is not None and user_id != expected_user_id:
                try:
                    bot.answer_callback_query(call.id, "Эта кнопка доступна только для победителя случайного события", show_alert=True)
                    logger.info(f"[START MENU] Показана ошибка пользователю {user_id} (кнопка для {expected_user_id})")
                except Exception as e:
                    logger.warning(f"[START MENU] Не удалось показать ошибку: {e}")
                logger.info(f"[START MENU] Пользователь {user_id} пытается использовать кнопку, предназначенную для {expected_user_id}")
                return
            
            # Отвечаем на callback только если проверка прошла
            safe_answer_callback_query(bot, call.id)

            logger.info(f"[START MENU] Обработка действия: {action}, user_id={user_id}")

            # Импортируем нужные функции один раз здесь
            from moviebot.bot.handlers.plan import show_schedule
            from moviebot.bot.handlers.payment import payment_command
            from moviebot.bot.handlers.series import handle_search, random_start, premieres_command, ticket_command, help_command, HELP_INTRO_TEXT
            from moviebot.bot.handlers.seasons import show_seasons_list

            # Обычный импорт settings_main
            from moviebot.bot.handlers.settings_main import settings_command

            # Обработка locked билетов (только в группах)
            if action == 'tickets_locked':
                text = "🎫 <b>Билеты в кино</b>\n\nВ групповых чатах загрузка билетов доступна с подпиской <b>💎 Movie Planner PRO</b>.\n\nИспользуйте /payment для оформления подписки."
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("💎 Movie Planner PRO", callback_data="payment:tariffs:personal"))
                markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                try:
                    bot.edit_message_text(
                        text=text,
                        chat_id=chat_id,
                        message_id=message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logger.warning(f"[START MENU] Не удалось отредактировать: {e}")
                    bot.send_message(
                        chat_id=chat_id,
                        text=text,
                        reply_markup=markup,
                        parse_mode='HTML',
                        message_thread_id=message_thread_id
                    )
                return
            
            if action == 'tickets':
                from moviebot.bot.handlers.series import show_cinema_sessions
                show_cinema_sessions(chat_id, user_id, None)
                return

            if action == 'seasons':
                # Сразу показываем отклик в сообщении — пользователь видит, что идёт загрузка
                try:
                    edit_kw = {'chat_id': chat_id, 'message_id': message_id, 'text': '⏳ Загружаю список сериалов...', 'parse_mode': 'HTML'}
                    bot.edit_message_text(**edit_kw)
                except Exception as edit_err:
                    logger.warning(f"[START MENU] Не удалось отредактировать на «Загружаю»: {edit_err}")
                show_seasons_list(
                    chat_id=chat_id,
                    user_id=user_id,
                    message_id=message_id,
                    message_thread_id=message_thread_id,
                    bot=bot
                )
                return  # Не удаляем сообщение, так как оно редактируется
            elif action == 'premieres':
                msg = call.message
                msg.text = '/premieres'
                premieres_command(msg)

            elif action == 'random':
                # Исправляем user_id в сообщении - используем call.from_user вместо call.message.from_user
                msg = call.message
                msg.from_user = call.from_user  # Используем правильный user_id из callback
                msg.text = '/random'
                random_start(msg)

            elif action == 'search':
                msg = call.message
                msg.text = '/search'
                handle_search(msg)

            elif action == 'schedule':
                msg = call.message
                msg.text = '/schedule'
                show_schedule(msg)

            elif action == 'tickets':
                from moviebot.bot.handlers.series import show_cinema_sessions
                show_cinema_sessions(chat_id, user_id, None)
                return

            elif action == 'payment':
                msg = call.message
                msg.text = '/payment'
                payment_command(msg)

            elif action == 'settings':
                msg = call.message
                msg.text = '/settings'
                msg.from_user = call.from_user  # пользователь, нажавший кнопку (сообщение от бота — from_user был бы бот)
                settings_command(msg)

            elif action == 'help':
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("🎬 Помощь по использованию бота", callback_data="help:bot_usage"))
                markup.add(InlineKeyboardButton("📖 Сценарии взаимодействия с сервисом", callback_data="help:scenarios"))
                markup.add(InlineKeyboardButton("💻 Работа с расширением", callback_data="help:extension"))
                markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                try:
                    bot.edit_message_text(
                        text=HELP_INTRO_TEXT,
                        chat_id=chat_id,
                        message_id=message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logger.warning(f"[START MENU] edit help intro: {e}")
                    bot.send_message(chat_id, HELP_INTRO_TEXT, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
                return

            # Удаляем старое меню для всех действий
            try:
                bot.delete_message(chat_id, message_id)
            except:
                pass

        except Exception as e:
            logger.error(f"[START MENU] Ошибка в обработчике: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Произошла ошибка", show_alert=True)
            except:
                pass

# Удаляем дублирующий обработчик back_to_start_menu - он уже зарегистрирован выше на уровне модуля

            user_id = call.from_user.id
            chat_id = call.message.chat.id
            message_id = call.message.message_id
            
            # Очищаем состояние планирования, если оно есть
            if user_id in user_plan_state:
                del user_plan_state[user_id]
                logger.info(f"[BACK TO MENU] Очищено состояние планирования для user_id={user_id}")

            # Та же логика подписки, что и в /start (теперь с группой)
            subscription_info = ""
            try:
                if call.message.chat.type == 'private':
                    sub = get_active_subscription(chat_id, user_id, 'personal')
                    if sub:
                        plan_type = sub.get('plan_type', 'all')
                        plan_name = "💎 Movie Planner PRO" if plan_type == 'all' else plan_type
                        subscription_info = f"\n\n<b>Ваша подписка:</b> {plan_name}\n"
                    else:
                        subscription_info = "\n\n<b>Базовая версия бота</b>\n"
                else:
                    group_sub = get_active_group_subscription_by_chat_id(chat_id)
                    if group_sub:
                        plan_type = group_sub.get('plan_type', 'all')
                        plan_name = "💎 Movie Planner PRO" if plan_type == 'all' else plan_type
                        subscription_info = f"\n\n<b>Подписка группы:</b> {plan_name}\n"
                    else:
                        subscription_info = "\n\n<b>Базовая версия бота</b>\n"
            except Exception as sub_error:
                logger.error(f"[BACK TO MENU] Ошибка получения информации о подписке: {sub_error}", exc_info=True)
                subscription_info = "\n\n<b>Базовая версия бота</b>\n"

            welcome_text = f"""
🎬 <b>Главное меню</b>{subscription_info}

💌 Чтобы добавить в базу фильм или сериал, пришлите в сообщении ссылку на страницу фильма или сериала на Кинопоиске в бот.

Выберите раздел из меню ниже ⬇
            """.strip()

            markup = InlineKeyboardMarkup()

            markup.row(
                InlineKeyboardButton("📺 Сериалы", callback_data="start_menu:seasons"),
                InlineKeyboardButton("📅 Премьеры", callback_data="start_menu:premieres")
            )
            markup.row(InlineKeyboardButton("🔍 Поиск", callback_data="start_menu:search"))
            markup.row(
                InlineKeyboardButton("🗄️ База", callback_data="start_menu:database"),
                InlineKeyboardButton("🗓️ Расписание", callback_data="start_menu:schedule")
            )
            tickets_text = "🎫 Билеты"
            tickets_callback = "start_menu:tickets"
            markup.row(
                InlineKeyboardButton("🤔 Что посмотреть?", callback_data="start_menu:what_to_watch"),
                InlineKeyboardButton(tickets_text, callback_data=tickets_callback)
            )
            markup.row(
                InlineKeyboardButton("💰", callback_data="start_menu:payment"),
                InlineKeyboardButton("💻", callback_data="start_menu:extension"),
                InlineKeyboardButton("⚙️", callback_data="start_menu:settings"),
                InlineKeyboardButton("❓", callback_data="start_menu:help")
            )

            bot.edit_message_text(
                text=welcome_text,
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )

        except Exception as e:
            logger.error(f"[BACK TO MENU] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка возврата в меню", show_alert=True)
            except:
                pass

# Удаляем дублирующую функцию register_start_handlers - она уже определена выше на строке 358