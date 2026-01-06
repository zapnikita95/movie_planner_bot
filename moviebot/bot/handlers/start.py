"""
Обработчики команды /start и главного меню
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from moviebot.database.db_operations import (
    get_active_subscription,
    get_active_group_subscription_by_chat_id,
    log_request
)
from moviebot.utils.helpers import has_tickets_access, has_recommendations_access
from moviebot.database.db_connection import db_lock, get_db_cursor
from moviebot.bot.handlers.seasons import count_episodes_for_watch_check, get_seasons_data, get_series_airing_status
from moviebot.api.kinopoisk_api import get_seasons_data  # Если нужно для проверки

logger = logging.getLogger(__name__)


def register_start_handlers(bot):
    """Регистрирует обработчики команды /start"""
    
    @bot.message_handler(commands=['start', 'menu'])
    def send_welcome(message):
        try:
            message_text = message.text or ""
            command_type = '/start' if message_text.startswith('/start') else '/menu'
            logger.info(f"[HANDLER] {command_type} вызван от {message.from_user.id}, chat_type={message.chat.type}, text='{message_text}'")
            username = message.from_user.username or f"user_{message.from_user.id}"
            log_request(message.from_user.id, username, '/start', message.chat.id)
            logger.info(f"Команда /start от пользователя {message.from_user.id}")
        except Exception as e:
            logger.error(f"[SEND_WELCOME] Ошибка в начале функции: {e}", exc_info=True)
            try:
                bot.reply_to(message, "❌ Произошла ошибка. Попробуйте еще раз.")
            except:
                pass
            return

        # Унифицированное приветствие для личных сообщений и групп
        subscription_info = ""
        
        if message.chat.type == 'private':
            # Проверяем личную подписку
            sub = get_active_subscription(message.chat.id, message.from_user.id, 'personal')
            if sub:
                plan_type = sub.get('plan_type', 'all')
                plan_names = {
                    'notifications': 'Уведомления о сериалах',
                    'recommendations': 'Рекомендации',
                    'tickets': 'Билеты',
                    'all': 'Все режимы'
                }
                plan_name = plan_names.get(plan_type, plan_type)
                subscription_info = f"\n\n💎 <b>Ваша подписка:</b> {plan_name}\n"
            else:
                subscription_info = "\n\n📦 <b>Базовая версия бота</b>\n"
        else:
            # Проверяем групповую подписку
            group_sub = get_active_group_subscription_by_chat_id(message.chat.id)
            if group_sub:
                plan_type = group_sub.get('plan_type', 'all')
                plan_names = {
                    'notifications': 'Уведомления о сериалах',
                    'recommendations': 'Рекомендации',
                    'tickets': 'Билеты',
                    'all': 'Все режимы'
                }
                plan_name = plan_names.get(plan_type, plan_type)
                subscription_info = f"\n\n💎 <b>Подписка группы:</b> {plan_name}\n"
            else:
                subscription_info = "\n\n📦 <b>Базовая версия бота</b>\n"
        
        welcome_text = f"""
🎬 <b>Главное меню</b>{subscription_info}

💌 Чтобы добавить в базу фильм или сериал, пришлите в сообщении ссылку на страницу фильма или сериала на кинопоиске в бот.

Выберите раздел из меню ниже ⬇
        """.strip()

        try:
            # Создаём меню с кнопками
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("📺 Сериалы", callback_data="start_menu:seasons"))
            markup.add(InlineKeyboardButton("📅 Премьеры", callback_data="start_menu:premieres"))
            markup.add(InlineKeyboardButton("🎲 Рандом", callback_data="start_menu:random"))
            markup.add(InlineKeyboardButton("🔍 Поиск фильмов и сериалов", callback_data="start_menu:search"))
            markup.add(InlineKeyboardButton("🗓️ Расписание", callback_data="start_menu:schedule"))
            # Добавляем кнопку Билеты всегда, но под замочком если нет подписки
            try:
                has_tickets = has_tickets_access(message.chat.id, message.from_user.id)
            except Exception as e:
                logger.error(f"[SEND_WELCOME] Ошибка при проверке доступа к билетам: {e}", exc_info=True)
                has_tickets = False
            
            if has_tickets:
                markup.add(InlineKeyboardButton("🎫 Билеты", callback_data="start_menu:tickets"))
            else:
                markup.add(InlineKeyboardButton("🔒 Билеты", callback_data="start_menu:tickets_locked"))
            markup.add(InlineKeyboardButton("💳 Оплата", callback_data="start_menu:payment"))
            markup.add(InlineKeyboardButton("⚙️ Настройки", callback_data="start_menu:settings"))
            markup.add(InlineKeyboardButton("❓ Помощь", callback_data="start_menu:help"))
            
            bot.reply_to(message, welcome_text, parse_mode='HTML', reply_markup=markup)
            logger.info(f"✅ Ответ на /start отправлен пользователю {message.from_user.id}")
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке ответа на /start: {e}", exc_info=True)
            try:
                bot.reply_to(message, "❌ Произошла ошибка при загрузке меню. Попробуйте еще раз.")
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data.startswith("start_menu:"))
    def start_menu_callback(call):
        """Обработчик выбора раздела из меню /start"""
        try:
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            action = call.data.split(":")[1]  # seasons, premieres, random, search, schedule, payment, help
            
            logger.info(f"[START MENU] Обработка действия: {action}, user_id={user_id}, chat_id={chat_id}")
            
            # Импортируем обработчики команд (они будут зарегистрированы в commands.py)
            from moviebot.bot.handlers.plan import show_schedule
            from moviebot.bot.handlers.payment import payment_command
            from moviebot.bot.handlers.series import handle_search, random_start, premieres_command, ticket_command, help_command
            # Команда /seasons обрабатывается через callback в series_callbacks.py
            # Используем importlib для обхода конфликта имен (есть и файл settings.py, и директория settings/)
            import importlib.util
            settings_spec = importlib.util.spec_from_file_location("settings_module", "moviebot/bot/handlers/settings.py")
            settings_module = importlib.util.module_from_spec(settings_spec)
            settings_spec.loader.exec_module(settings_module)
            settings_command = settings_module.settings_command
            
            # Используем существующее сообщение и устанавливаем текст команды
            message = call.message
            message.text = None  # Очищаем текст
            
            # Обрабатываем tickets_locked ПЕРВЫМ, чтобы не перехватывалось другими обработчиками
            if action == 'tickets_locked':
                logger.info(f"[START MENU] Обработка tickets_locked для user_id={user_id}")
                # Показываем сообщение о необходимости подписки
                text = "🎫 <b>Билеты в кино</b>\n\n"
                text += "Вы можете загружать билеты и получать их в боте прямо перед сеансом с подпиской <b>\"Билеты\"</b>.\n\n"
                text += "Используйте /payment для оформления подписки."
                
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🎫 К подписке Билеты", callback_data="payment:tariffs:personal"))
                markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                
                try:
                    bot.edit_message_text(
                        text,
                        chat_id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                    logger.info(f"[START MENU] Сообщение о билетах отправлено для user_id={user_id}")
                except Exception as e:
                    logger.warning(f"[START MENU] Не удалось отредактировать сообщение, отправляем новое: {e}")
                    bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
                return
            
            # Вызываем соответствующую команду
            elif action == 'seasons':
                logger.info(f"[START MENU] Показ списка сериалов для user_id={user_id}, chat_id={chat_id}")
                
                try:
                    with db_lock:
                        cursor.execute("""
                            SELECT m.kp_id, m.title, m.year
                            FROM movies m
                            WHERE m.chat_id = %s AND m.is_series = TRUE
                            ORDER BY m.added_at DESC
                            LIMIT 50  # Чтобы не перегружать, если много
                        """, (chat_id,))
                        series_list = cursor.fetchall()
                    
                    if not series_list:
                        text = "📺 <b>Сериалы</b>\n\nВ базе пока нет сериалов.\nДобавьте их через поиск или ссылку с Кинопоиска."
                        markup = InlineKeyboardMarkup(row_width=2)
                        markup.add(
                            InlineKeyboardButton("🔍 Поиск сериалов", callback_data="search_type:series"),
                            InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu")
                        )
                    else:
                        text = f"📺 <b>Ваши сериалы ({len(series_list)})</b>\n\n"
                        markup = InlineKeyboardMarkup(row_width=1)
                        
                        for row in series_list:
                            kp_id = row[0] if isinstance(row, tuple) else row.get('kp_id')
                            title = row[1] if isinstance(row, tuple) else row.get('title')
                            year = row[2] if isinstance(row, tuple) else row.get('year', '—')
                            
                            button_text = f"📺 {title} ({year})"
                            if len(button_text) > 60:
                                button_text = button_text[:57] + "..."
                            markup.add(InlineKeyboardButton(button_text, callback_data=f"series_track:{kp_id}"))
                        
                        # Добавляем кнопку для просмотренных сериалов
                        markup.add(InlineKeyboardButton("👀 Просмотренные сериалы", callback_data="start_menu:watched_seasons"))
                        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                    
                    bot.edit_message_text(
                        text,
                        chat_id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                    logger.info(f"[START MENU] Список сериалов отправлен: {len(series_list) if series_list else 0} шт.")
                    
                except Exception as e:
                    logger.error(f"[START MENU] Ошибка при загрузке сериалов: {e}", exc_info=True)
                    bot.edit_message_text(
                        "❌ Ошибка при загрузке списка сериалов. Попробуйте позже.",
                        chat_id,
                        call.message.message_id
                    )
                return
                
            elif action == 'watched_seasons':
                logger.info(f"[START MENU] Показ полностью просмотренных сериалов для user_id={user_id}, chat_id={chat_id}")
                
                try:
                    watched_series = []
                    
                    with db_lock:
                        cursor = get_db_cursor()
                        cursor.execute("""
                            SELECT m.id, m.kp_id, m.title, m.year
                            FROM movies m
                            WHERE m.chat_id = %s AND m.is_series = TRUE
                            ORDER BY m.added_at DESC
                            LIMIT 50
                        """, (chat_id,))
                        all_series = cursor.fetchall()
                    
                    for row in all_series:
                        film_id = row[0] if isinstance(row, tuple) else row.get('id')
                        kp_id = row[1] if isinstance(row, tuple) else row.get('kp_id')
                        title = row[2] if isinstance(row, tuple) else row.get('title')
                        year = row[3] if isinstance(row, tuple) else row.get('year', '—')
                        
                        seasons_data = get_seasons_data(kp_id)
                        if not seasons_data:
                            continue
                        
                        is_airing, _ = get_series_airing_status(kp_id)
                        
                        with db_lock:
                            cursor.execute("""
                                SELECT season_number, episode_number
                                FROM series_tracking
                                WHERE chat_id = %s AND film_id = %s AND watched = TRUE
                            """, (chat_id, film_id))
                            watched_rows = cursor.fetchall()
                            watched_set = set((str(r[0]), str(r[1])) for r in watched_rows)  # str на всякий случай
                        
                        _, watched_episodes = count_episodes_for_watch_check(
                            seasons_data, is_airing, watched_set, chat_id, film_id, user_id
                        )
                        total_episodes, _ = count_episodes_for_watch_check(
                            seasons_data, is_airing, set(), chat_id, film_id, user_id
                        )  # пустой set для total без watched
                        
                        if total_episodes > 0 and total_episodes == watched_episodes:
                            watched_series.append({'kp_id': kp_id, 'title': title, 'year': year})
                    
                    if not watched_series:
                        text = "👀 <b>Просмотренные сериалы</b>\n\nПока нет полностью просмотренных сериалов.\nОтмечайте эпизоды в обычном списке сериалов."
                        markup = InlineKeyboardMarkup(row_width=1)
                        markup.add(InlineKeyboardButton("📺 К списку сериалов", callback_data="start_menu:seasons"))
                        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                    else:
                        text = f"👀 <b>Просмотренные сериалы ({len(watched_series)})</b>\n\n"
                        markup = InlineKeyboardMarkup(row_width=1)
                        for series in watched_series:
                            button_text = f"📺 {series['title']} ({series['year']})"
                            if len(button_text) > 60:
                                button_text = button_text[:57] + "..."
                            markup.add(InlineKeyboardButton(button_text, callback_data=f"series_track:{series['kp_id']}"))
                        markup.add(InlineKeyboardButton("📺 К списку сериалов", callback_data="start_menu:seasons"))
                        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                    
                    bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    
                except Exception as e:
                    logger.error(f"[WATCHED SEASONS] Ошибка: {e}", exc_info=True)
                    bot.edit_message_text("❌ Ошибка при загрузке просмотренных сериалов.", chat_id, call.message.message_id)
                return
            elif action == 'premieres':
                message.text = '/premieres'
                premieres_command(message)
            elif action == 'random':
                message.text = '/random'
                # Исправляем from_user.id на реальный user_id пользователя
                message.from_user.id = user_id
                random_start(message)
            elif action == 'search':
                # Создаем правильное сообщение с user_id пользователя
                message.text = '/search'
                # Исправляем from_user.id на реальный user_id пользователя
                message.from_user.id = user_id
                handle_search(message)
            elif action == 'schedule':
                message.text = '/schedule'
                show_schedule(message)
            elif action == 'tickets':
                # Проверяем доступ перед вызовом команды
                if not has_tickets_access(chat_id, user_id):
                    # Показываем сообщение о необходимости подписки
                    text = "🎫 <b>Билеты в кино</b>\n\n"
                    text += "Вы можете загружать билеты и получать их в боте прямо перед сеансом с подпиской <b>\"Билеты\"</b>.\n\n"
                    text += "Используйте /payment для оформления подписки."
                    
                    markup = InlineKeyboardMarkup()
                    markup.add(InlineKeyboardButton("🎫 К подписке Билеты", callback_data="payment:tariffs:personal"))
                    markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                    
                    try:
                        bot.edit_message_text(
                            text,
                            chat_id,
                            call.message.message_id,
                            reply_markup=markup,
                            parse_mode='HTML'
                        )
                    except Exception as e:
                        logger.warning(f"[START MENU] Не удалось отредактировать сообщение, отправляем новое: {e}")
                        bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
                    return
                else:
                    message.text = '/ticket'
                    ticket_command(message)
            elif action == 'payment':
                message.text = '/payment'
                payment_command(message)
            elif action == 'settings':
                message.text = '/settings'
                settings_command(message)
            elif action == 'help':
                message.text = '/help'
                help_command(message)
            elif action == 'shazam':
                # Импортируем handler для КиноШазам
                from moviebot.bot.handlers.shazam import shazam_start_callback
                # Создаем фиктивный callback для вызова
                class FakeCall:
                    def __init__(self):
                        self.id = "fake"
                        self.from_user = call.from_user
                        self.message = call.message
                        self.data = "start_menu:shazam"
                fake_call = FakeCall()
                shazam_start_callback(fake_call)
            
            # Удаляем сообщение с меню после успешной отправки нового сообщения
            # (только если не было return выше)
            try:
                bot.delete_message(chat_id, call.message.message_id)
            except:
                pass
            
            logger.info(f"[START MENU] Выбран раздел: {action} для пользователя {user_id}")
        except Exception as e:
            logger.error(f"[START MENU] Ошибка в start_menu_callback: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data == "back_to_start_menu")
    def back_to_start_menu_callback(call):
        """Обработчик кнопки возврата в главное меню"""
        try:
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Создаем сообщение с главным меню
            welcome_text = """
🎬 <b>Главное меню</b>

💌 Чтобы добавить в базу фильм или сериал, пришлите в сообщении ссылку на страницу фильма или сериала на кинопоиске в бот.

Выберите раздел из меню ниже ⬇
            """.strip()

            # Создаём меню с кнопками
            markup = InlineKeyboardMarkup(row_width=1)
            # КиноШазам доступен только с подпиской Рекомендации или Полная
            try:
                if has_recommendations_access(chat_id, user_id):
                    markup.add(InlineKeyboardButton("🔮 КиноШазам", callback_data="start_menu:shazam"))
                else:
                    markup.add(InlineKeyboardButton("🔒 КиноШазам", callback_data="start_menu:shazam"))
            except Exception as e:
                logger.warning(f"Ошибка при проверке доступа к КиноШазам для user_id={user_id}: {e}")
                markup.add(InlineKeyboardButton("🔒 КиноШазам", callback_data="start_menu:shazam"))
            markup.add(InlineKeyboardButton("📺 Сериалы", callback_data="start_menu:seasons"))
            markup.add(InlineKeyboardButton("📅 Премьеры", callback_data="start_menu:premieres"))
            markup.add(InlineKeyboardButton("🎲 Рандом", callback_data="start_menu:random"))
            markup.add(InlineKeyboardButton("🔍 Поиск фильмов и сериалов", callback_data="start_menu:search"))
            markup.add(InlineKeyboardButton("🗓️ Расписание", callback_data="start_menu:schedule"))
            # Добавляем кнопку Билеты всегда, но под замочком если нет подписки
            try:
                if has_tickets_access(chat_id, user_id):
                    markup.add(InlineKeyboardButton("🎫 Билеты", callback_data="start_menu:tickets"))
                else:
                    markup.add(InlineKeyboardButton("🔒 Билеты", callback_data="start_menu:tickets_locked"))
            except Exception as e:
                # В случае ошибки всегда показываем заблокированную версию
                logger.warning(f"Ошибка при проверке доступа к билетам для user_id={user_id}: {e}")
                markup.add(InlineKeyboardButton("🔒 Билеты", callback_data="start_menu:tickets_locked"))
            markup.add(InlineKeyboardButton("💳 Оплата", callback_data="start_menu:payment"))
            markup.add(InlineKeyboardButton("⚙️ Настройки", callback_data="start_menu:settings"))
            markup.add(InlineKeyboardButton("❓ Помощь", callback_data="start_menu:help"))
            
            # Редактируем сообщение или отправляем новое
            try:
                bot.edit_message_text(
                    welcome_text,
                    chat_id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
            except:
                # Если не удалось отредактировать, отправляем новое сообщение
                bot.send_message(chat_id, welcome_text, reply_markup=markup, parse_mode='HTML')
            
            logger.info(f"[BACK TO MENU] Пользователь {user_id} вернулся в главное меню")
        except Exception as e:
            logger.error(f"[BACK TO MENU] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
            except:
                pass

