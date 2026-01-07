"""
Обработчики команды /start и главного меню
"""
import logging
from telebot_instance.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.bot.bot_init import bot_instance  # ТОЛЬКО bot_instance
from moviebot_instance.database.db_operations import (
    get_active_subscription,
    get_active_group_subscription_by_chat_id,
    log_request
)
from moviebot_instance.utils.helpers import has_tickets_access, has_recommendations_access

logger = logging.getLogger(__name__)

logger.info("[START.PY] Модуль start.py загружен — глобальные обработчики зарегистрированы")


# ==================== ГЛОБАЛЬНЫЕ ОБРАБОТЧИКИ ====================

@bot_instance.message_handler(commands=['start', 'menu'])
def send_welcome(message):
    logger.info(f"[START] СРАБОТАЛ /start от user_id={message.from_user.id}, chat_id={message.chat.id}")

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
            bot_instance.reply_to(message, "❌ Произошла ошибка. Попробуйте еще раз.")
        except:
            pass
        return

    # Унифицированное приветствие для личных сообщений и групп
    subscription_info = ""
    
    if message.chat.type == 'private':
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
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Проверка доступа к рекомендациям (Нативный поиск)
        has_shazam_access = has_recommendations_access(message.chat.id, message.from_user.id)
        
        # Кнопки до Рандома
        markup.add(InlineKeyboardButton("📺 Сериалы", callback_data="start_menu:seasons"))
        markup.add(InlineKeyboardButton("📅 Премьеры", callback_data="start_menu:premieres"))
        markup.add(InlineKeyboardButton("🎲 Рандом", callback_data="start_menu:random"))
        
        # Нативный поиск — сразу после Рандома
        if has_shazam_access:
            markup.add(InlineKeyboardButton("🔮 Нативный поиск", callback_data="shazam:start"))
        else:
            markup.add(InlineKeyboardButton("🔒 Нативный поиск", callback_data="shazam:start"))
        
        # Остальные кнопки
        markup.add(InlineKeyboardButton("🔍 Поиск фильмов и сериалов", callback_data="start_menu:search"))
        markup.add(InlineKeyboardButton("🗓️ Расписание", callback_data="start_menu:schedule"))

        # Проверка билетов
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
        
        bot_instance.reply_to(message, welcome_text, parse_mode='HTML', reply_markup=markup)
        logger.info(f"✅ Ответ на /start отправлен пользователю {message.from_user.id}")

    except Exception as e:
        logger.error(f"❌ Ошибка при отправке ответа на /start: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "❌ Произошла ошибка при загрузке меню. Попробуйте еще раз.")
        except:
            pass


@bot_instance.callback_query_handler(func=lambda call: call.data.startswith("start_menu:"))
def start_menu_callback(call):
    try:
        from moviebot_instance.bot_instance.bot_init import safe_answer_callback_query
        safe_answer_callback_query(bot, call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        message_thread_id = getattr(call.message, 'message_thread_id', None)
        action = call.data.split(":")[1]

        logger.info(f"[START MENU] Обработка действия: {action}, user_id={user_id}, chat_id={chat_id}")

        # Убрали импорт seasons_command (его нет)
        from moviebot_instance.bot_instance.handlers.plan import show_schedule
        from moviebot_instance.bot_instance.handlers.payment import payment_command
        from moviebot_instance.bot_instance.handlers.series import handle_search, random_start, premieres_command, ticket_command, help_command

        import importlib.util
        settings_spec = importlib.util.spec_from_file_location("settings_module", "moviebot/bot/handlers/settings.py")
        settings_module = importlib.util.module_from_spec(settings_spec)
        settings_spec.loader.exec_module(settings_module)
        settings_command = settings_module.settings_command

        if action == 'tickets_locked':
            logger.info(f"[START MENU] Обработка tickets_locked для user_id={user_id}")
            text = "🎫 <b>Билеты в кино</b>\n\nВы можете загружать билеты и получать их в боте прямо перед сеансом с подпиской <b>\"Билеты\"</b>.\n\nИспользуйте /payment для оформления подписки."
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🎫 К подписке Билеты", callback_data="payment:tariffs:personal"))
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            try:
                bot_instance.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
            except Exception as e:
                logger.warning(f"[START MENU] Не удалось отредактировать сообщение: {e}")
                bot_instance.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
            return

        if action == 'seasons':
            bot_instance.answer_callback_query(call.id, "⏳ Загружаем сериалы и сезоны...")  # ← прелоадер (теперь bot)
            from moviebot_instance.bot_instance.handlers.seasons import show_seasons_list
            show_seasons_list(chat_id, user_id, message_id=message_id)

        elif action == 'premieres':
            message = call.message
            message.text = '/premieres'
            premieres_command(message)
        elif action == 'random':
            message = call.message
            message.text = '/random'
            message.from_user.id = user_id
            random_start(message)
        elif action == 'search':
            message = call.message
            message.text = '/search'
            message.from_user.id = user_id
            handle_search(message)
        elif action == 'schedule':
            message = call.message
            message.text = '/schedule'
            show_schedule(message)
        elif action == 'tickets':
            if not has_tickets_access(chat_id, user_id):
                text = "🎫 <b>Билеты в кино</b>\n\nВы можете загружать билеты и получать их в боте прямо перед сеансом с подпиской <b>\"Билеты\"</b>.\n\nИспользуйте /payment для оформления подписки."
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🎫 К подписке Билеты", callback_data="payment:tariffs:personal"))
                markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                try:
                    bot_instance.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
                except Exception as e:
                    logger.warning(f"[START MENU] Не удалось отредактировать сообщение: {e}")
                    bot_instance.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
                return
            else:
                message = call.message
                message.text = '/ticket'
                ticket_command(message)
        elif action == 'payment':
            message = call.message
            message.text = '/payment'
            payment_command(message)
        elif action == 'settings':
            message = call.message
            message.text = '/settings'
            settings_command(message)
        elif action == 'help':
            message = call.message
            message.text = '/help'
            help_command(message)

        # Удаляем старое меню только если не seasons (там мы уже отредактировали)
        if action != 'seasons':
            try:
                bot_instance.delete_message(chat_id, message_id)
            except:
                pass

        logger.info(f"[START MENU] Выбран раздел: {action} для пользователя {user_id}")
    except Exception as e:
        logger.error(f"[START MENU] Ошибка в start_menu_callback: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
        except:
            pass
        
@bot_instance.callback_query_handler(func=lambda call: call.data == "back_to_start_menu")
def back_to_start_menu_callback(call):
    """Универсальный обработчик для всех кнопок 'Назад в меню'"""
    try:
        bot_instance.answer_callback_query(call.id, "⏳ Возвращаемся...")  # ← прелоадер через bot

        user_id = call.from_user.id
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        message_thread_id = getattr(call.message, 'message_thread_id', None)
        
        logger.info(f"[BACK TO START MENU] user_id={user_id}, chat_id={chat_id}")

        # === ПОЛНАЯ информация о подписке (как в /start) ===
        subscription_info = ""
        if call.message.chat.type == 'private':
            sub = get_active_subscription(chat_id, user_id, 'personal')
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

        welcome_text = f"""
🎬 <b>Главное меню</b>{subscription_info}

💌 Чтобы добавить в базу фильм или сериал, пришлите в сообщении ссылку на страницу фильма или сериала на кинопоиске в бот.

Выберите раздел из меню ниже ⬇
        """.strip()

        markup = InlineKeyboardMarkup(row_width=1)
        
        has_shazam_access = has_recommendations_access(chat_id, user_id)
        
        markup.add(InlineKeyboardButton("📺 Сериалы", callback_data="start_menu:seasons"))
        markup.add(InlineKeyboardButton("📅 Премьеры", callback_data="start_menu:premieres"))
        markup.add(InlineKeyboardButton("🎲 Рандом", callback_data="start_menu:random"))
        
        if has_shazam_access:
            markup.add(InlineKeyboardButton("🔮 Нативный поиск", callback_data="shazam:start"))
        else:
            markup.add(InlineKeyboardButton("🔒 Нативный поиск", callback_data="shazam:start"))
        
        markup.add(InlineKeyboardButton("🔍 Поиск фильмов и сериалов", callback_data="start_menu:search"))
        markup.add(InlineKeyboardButton("🗓️ Расписание", callback_data="start_menu:schedule"))
        
        try:
            if has_tickets_access(chat_id, user_id):
                markup.add(InlineKeyboardButton("🎫 Билеты", callback_data="start_menu:tickets"))
            else:
                markup.add(InlineKeyboardButton("🔒 Билеты", callback_data="start_menu:tickets_locked"))
        except Exception as e:
            logger.warning(f"[BACK TO MENU] Ошибка проверки билетов: {e}")
            markup.add(InlineKeyboardButton("🔒 Билеты", callback_data="start_menu:tickets_locked"))
            
        markup.add(InlineKeyboardButton("💳 Оплата", callback_data="start_menu:payment"))
        markup.add(InlineKeyboardButton("⚙️ Настройки", callback_data="start_menu:settings"))
        markup.add(InlineKeyboardButton("❓ Помощь", callback_data="start_menu:help"))
        
        bot_instance.edit_message_text(
            welcome_text,
            chat_id,
            message_id,
            reply_markup=markup,
            parse_mode='HTML',
            message_thread_id=message_thread_id
        )
        
        logger.info(f"[BACK TO MENU] Главное меню показано пользователю {user_id}")
        
    except Exception as e:
        logger.error(f"[BACK TO MENU] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка возврата в меню", show_alert=True)
        except:
            pass