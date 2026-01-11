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
from moviebot.utils.helpers import has_tickets_access, has_recommendations_access

from moviebot.bot.bot_init import safe_answer_callback_query

logger = logging.getLogger(__name__)

logger.info("[START.PY] Модуль start.py загружен")


def register_start_handlers(bot):
    """Регистрация всех обработчиков из этого модуля"""

    @bot.message_handler(commands=['start', 'menu'])
    def send_welcome(message):
        logger.info(f"[START] СРАБОТАЛ /start от user_id={message.from_user.id}, chat_id={message.chat.id}")

        try:
            message_text = message.text or ""
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

        # Информация о подписке
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

💌 Чтобы добавить в базу фильм или сериал, пришлите в сообщении ссылку на страницу фильма или сериала на Кинопоиске в бот.

Выберите раздел из меню ниже ⬇
        """.strip()



        try:
            # В back_to_start_menu_callback (аналогично, внутри try после markup = InlineKeyboardMarkup())
            markup = InlineKeyboardMarkup()

            has_shazam_access = has_recommendations_access(chat_id, user_id)
            has_tickets = has_tickets_access(chat_id, user_id)

            # Строка 1
            markup.row(
                InlineKeyboardButton("📺 Сериалы", callback_data="start_menu:seasons"),
                InlineKeyboardButton("📅 Премьеры", callback_data="start_menu:premieres")
            )

            # Строка 2
            markup.row(
                InlineKeyboardButton("🎲 Рандом", callback_data="start_menu:random")
            )

            # Строка 3
            shazam_text = "🔮 Нативный поиск" if has_shazam_access else "🔒 Нативный поиск"
            markup.row(
                InlineKeyboardButton(shazam_text, callback_data="shazam:start"),
                InlineKeyboardButton("🔍 Поиск", callback_data="start_menu:search")
            )

            # Строка 4
            tickets_text = "🎫 Билеты" if has_tickets else "🔒 Билеты"
            tickets_callback = "start_menu:tickets" if has_tickets else "start_menu:tickets_locked"
            markup.row(
                InlineKeyboardButton("🗓️ Расписание", callback_data="start_menu:schedule"),
                InlineKeyboardButton(tickets_text, callback_data=tickets_callback)
            )

            # Строка 5
            markup.row(
                InlineKeyboardButton("💰", callback_data="start_menu:payment"),
                InlineKeyboardButton("⚙️", callback_data="start_menu:settings"),
                InlineKeyboardButton("❓", callback_data="start_menu:help")
            )
            logger.info(f"✅ Ответ на /start отправлен пользователю {message.from_user.id}")

        except Exception as e:
            logger.error(f"❌ Ошибка при отправке меню: {e}", exc_info=True)
            try:
                bot.reply_to(message, "❌ Ошибка при загрузке меню. Попробуйте позже.")
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data.startswith("start_menu:"))
    def start_menu_callback(call):
        try:
            safe_answer_callback_query(bot, call.id)

            user_id = call.from_user.id
            chat_id = call.message.chat.id
            message_id = call.message.message_id
            message_thread_id = getattr(call.message, 'message_thread_id', None)
            action = call.data.split(":")[1]

            logger.info(f"[START MENU] Обработка действия: {action}, user_id={user_id}")

            # Импортируем нужные функции один раз здесь
            from moviebot.bot.handlers.plan import show_schedule
            from moviebot.bot.handlers.payment import payment_command
            from moviebot.bot.handlers.series import handle_search, random_start, premieres_command, ticket_command, help_command
            from moviebot.bot.handlers.seasons import show_seasons_list

            # Динамический импорт settings
            import importlib.util
            settings_path = "moviebot/bot/handlers/settings.py"
            spec = importlib.util.spec_from_file_location("settings_module", settings_path)
            settings_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(settings_module)
            settings_command = settings_module.settings_command

            # Обработка locked билетов
            if action == 'tickets_locked':
                text = "🎫 <b>Билеты в кино</b>\n\nВы можете загружать билеты и получать их в боте прямо перед сеансом с подпиской <b>\"Билеты\"</b>.\n\nИспользуйте /payment для оформления подписки."
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("🎫 К подписке Билеты", callback_data="payment:tariffs:personal"))
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

            if action == 'seasons':
                bot.answer_callback_query(call.id, "⏳ Загружаем сериалы и сезоны...")
                show_seasons_list(
                    chat_id=chat_id,
                    user_id=user_id,
                    message_id=message_id,
                    message_thread_id=message_thread_id,
                    bot=bot
                )
            elif action == 'premieres':
                msg = call.message
                msg.text = '/premieres'
                premieres_command(msg)

            elif action == 'random':
                msg = call.message
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
                if not has_tickets_access(chat_id, user_id):
                    text = "🎫 <b>Билеты в кино</b>\n\nВы можете загружать билеты и получать их в боте прямо перед сеансом с подпиской <b>\"Билеты\"</b>.\n\nИспользуйте /payment для оформления подписки."
                    markup = InlineKeyboardMarkup()
                    markup.add(InlineKeyboardButton("🎫 К подписке Билеты", callback_data="payment:tariffs:personal"))
                    markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                    try:
                        bot.edit_message_text(
                            text=text,
                            chat_id=chat_id,
                            message_id=message_id,
                            reply_markup=markup,
                            parse_mode='HTML'
                        )
                    except:
                        bot.send_message(
                            chat_id=chat_id,
                            text=text,
                            reply_markup=markup,
                            parse_mode='HTML',
                            message_thread_id=message_thread_id
                        )
                    return
                else:
                    msg = call.message
                    msg.text = '/ticket'
                    ticket_command(msg)

            elif action == 'payment':
                msg = call.message
                msg.text = '/payment'
                payment_command(msg)

            elif action == 'settings':
                msg = call.message
                msg.text = '/settings'
                settings_command(msg)

            elif action == 'help':
                msg = call.message
                msg.text = '/help'
                help_command(msg)

            # Удаляем старое меню, если не перешли в seasons
            if action != 'seasons':
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

    @bot.callback_query_handler(func=lambda call: call.data == "back_to_start_menu")
    def back_to_start_menu_callback(call):
        try:
            bot.answer_callback_query(call.id, "⏳ Возвращаемся...")

            user_id = call.from_user.id
            chat_id = call.message.chat.id
            message_id = call.message.message_id

            # Та же логика подписки, что и в /start
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

💌 Чтобы добавить в базу фильм или сериал, пришлите в сообщении ссылку на страницу фильма или сериала на Кинопоиске в бот.

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

            if has_tickets_access(chat_id, user_id):
                markup.add(InlineKeyboardButton("🎫 Билеты", callback_data="start_menu:tickets"))
            else:
                markup.add(InlineKeyboardButton("🔒 Билеты", callback_data="start_menu:tickets_locked"))

            markup.add(InlineKeyboardButton("💳 Оплата", callback_data="start_menu:payment"))
            markup.add(InlineKeyboardButton("⚙️ Настройки", callback_data="start_menu:settings"))
            markup.add(InlineKeyboardButton("❓ Помощь", callback_data="start_menu:help"))

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