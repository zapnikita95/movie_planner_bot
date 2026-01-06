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
            # Кнопка КиноШазам (в самом верху)
            has_shazam_access = has_recommendations_access(message.chat.id, message.from_user.id)
            if has_shazam_access:
                markup.add(InlineKeyboardButton("🔮 КиноШазам", callback_data="shazam:start"))
            else:
                markup.add(InlineKeyboardButton("🔒 КиноШазам", callback_data="shazam:start"))
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
            from moviebot.bot.handlers.seasons import seasons_command
            from moviebot.bot.handlers.plan import show_schedule
            from moviebot.bot.handlers.payment import payment_command
            from moviebot.bot.handlers.series import handle_search, random_start, premieres_command, ticket_command, help_command
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
            if action == 'seasons':
                message.text = '/seasons'
                seasons_command(message)
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
            # Кнопка КиноШазам (в самом верху)
            has_shazam_access = has_recommendations_access(chat_id, user_id)
            if has_shazam_access:
                markup.add(InlineKeyboardButton("🔮 КиноШазам", callback_data="shazam:start"))
            else:
                markup.add(InlineKeyboardButton("🔒 КиноШазам", callback_data="shazam:start"))
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

