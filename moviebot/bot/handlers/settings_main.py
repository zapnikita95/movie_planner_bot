from moviebot.bot.bot_init import bot
"""
Обработчики команды /settings - настройки бота
"""
import logging
from moviebot.config import PLANS_TZ
import random
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton


from moviebot.database.db_operations import (

    log_request, set_user_timezone,
    get_watched_emojis, get_user_timezone, get_notification_settings, set_notification_setting
)
import re
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock

from moviebot.utils.helpers import has_recommendations_access, has_notifications_access

from moviebot.config import PLANS_TZ

from moviebot.states import (

    user_settings_state, settings_messages,
    dice_game_state, user_import_state
)
from datetime import datetime, timedelta

import pytz

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


def settings_command(message):
    """Команда /settings - настройки"""
    try:
        user_id = message.from_user.id if message.from_user else None
        chat_id = message.chat.id if message.chat else None
    except Exception:
        user_id = None
        chat_id = None

    logger.info(f"[SETTINGS COMMAND] START /settings: user_id={user_id}, chat_id={chat_id}")
    logger.info(f"[HANDLER] /settings вызван от {user_id}")
    try:
        chat_id = message.chat.id
        user_id = message.from_user.id
        username = message.from_user.username or f"user_{user_id}"
        log_request(user_id, username, '/settings', chat_id)
        logger.info(f"Команда /settings от пользователя {user_id}")
        
        # Сначала показываем меню выбора действия
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("🕐 Выбрать часовой пояс", callback_data="settings:timezone"))
        
        # Проверяем доступ к настройкам напоминаний (требуется подписка на уведомления)
        if has_notifications_access(chat_id, user_id):
            markup.add(InlineKeyboardButton("⏰ Настройки напоминаний", callback_data="settings:notifications"))
        else:
            markup.add(InlineKeyboardButton("🔒 Настройки напоминаний", callback_data="settings:notifications_locked"))
        
        # Проверяем доступ к импорту базы (требуется подписка на рекомендации)
        if has_recommendations_access(chat_id, user_id):
            markup.add(InlineKeyboardButton("📥 Импорт базы из Кинопоиска", callback_data="settings:import"))
        else:
            markup.add(InlineKeyboardButton("🔒 Импорт базы из Кинопоиска", callback_data="settings:import_locked"))
        
        # Проверяем, является ли чат личным (случайные события доступны только в группах)
        is_private = message.chat.type == 'private'
        if is_private:
            markup.add(InlineKeyboardButton("🔒 Случайные события", callback_data="settings:random_events_locked"))
        else:
            markup.add(InlineKeyboardButton("🎲 Случайные события", callback_data="settings:random_events"))
        markup.add(InlineKeyboardButton("✏️ Редактировать записи", callback_data="settings:edit"))
        markup.add(InlineKeyboardButton("🗑️ Очистка базы", callback_data="settings:clean"))
        markup.add(InlineKeyboardButton("👥 Участие", callback_data="settings:join"))
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
        sent = bot.send_message(chat_id,
            f"⚙️ <b>Настройки</b>\n\n"
            f"Выберите, что хотите настроить:",
            reply_markup=markup,
            parse_mode='HTML')
        
        logger.info(f"Настройки открыты для {user_id}, msg_id: {sent.message_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в /settings: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /settings")
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("settings:"))
def handle_settings_callback(call):
    """Обработчик callback для настроек"""
    try:
        raw_data = call.data
        user_id = call.from_user.id if call.from_user else None
        chat_id = call.message.chat.id if call.message and call.message.chat else None
        logger.info(f"[SETTINGS CALLBACK DEBUG] raw callback_data={raw_data}, user_id={user_id}, chat_id={chat_id}")
    except Exception:
        logger.warning("[SETTINGS CALLBACK DEBUG] Не удалось залогировать raw callback_data/user_id/chat_id")

    logger.info(f"[SETTINGS CALLBACK] ===== НАЧАЛО ОБРАБОТКИ =====")
    logger.info(f"[SETTINGS CALLBACK] callback_id={call.id}, message_id={call.message.message_id if call.message else None}")
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        action = call.data.split(":", 1)[1]
        is_private = call.message.chat.type == 'private'
        
        logger.info(f"[SETTINGS CALLBACK] Получен callback от {user_id}, action={action}, chat_id={chat_id}, is_private={is_private}, callback_data={call.data}")
        
        # Вызываем answer_callback_query в самом начале (как в рабочей версии)
        # Но сначала обрабатываем заблокированные кнопки
        if action == "notifications_locked":
            # Заблокированная кнопка настроек напоминаний
            try:
                bot.answer_callback_query(
                    call.id,
                    "⏰ Настройки напоминаний доступны с подпиской 🔔 Уведомления или 📦 Все режимы. Подключите подписку через /payment",
                    show_alert=True
                )
            except Exception as e:
                logger.error(f"[SETTINGS] Ошибка при ответе на callback для notifications_locked: {e}")
            return
        
        if action == "import_locked":
            # Заблокированная кнопка импорта базы
            # Проверяем доступ еще раз для логирования
            has_access = has_recommendations_access(chat_id, user_id)
            logger.info(f"[SETTINGS] import_locked: user_id={user_id}, chat_id={chat_id}, has_access={has_access}")
            if not has_access:
                # Дополнительная диагностика для групповых чатов
                if chat_id < 0:
                    from moviebot.database.db_operations import get_active_group_subscription_by_chat_id, get_subscription_members
                    group_sub = get_active_group_subscription_by_chat_id(chat_id)
                    if group_sub:
                        subscription_id = group_sub.get('id')
                        plan_type = group_sub.get('plan_type')
                        group_size = group_sub.get('group_size')
                        expires_at = group_sub.get('expires_at')
                        logger.warning(f"[SETTINGS] import_locked: найдена групповая подписка subscription_id={subscription_id}, plan_type={plan_type}, group_size={group_size}, expires_at={expires_at}, но доступ не разрешен")
                        if group_size is not None and subscription_id:
                            try:
                                members = get_subscription_members(subscription_id)
                                logger.warning(f"[SETTINGS] import_locked: участники подписки {subscription_id}: {members}, user_id={user_id} в списке: {user_id in members if members else False}")
                            except Exception as e:
                                logger.error(f"[SETTINGS] import_locked: ошибка получения участников: {e}", exc_info=True)
            try:
                bot.answer_callback_query(
                    call.id,
                    "📥 Импорт базы из Кинопоиска доступен с подпиской 🎯 Рекомендации или 📦 Все режимы. Подключите подписку через /payment",
                    show_alert=True
                )
            except Exception as e:
                logger.error(f"[SETTINGS] Ошибка при ответе на callback для import_locked: {e}")
            return
        
        if action == "random_events_locked":
            # Показываем сообщение о том, что раздел доступен только в групповых чатах
            try:
                bot.answer_callback_query(
                    call.id,
                    "🎲 Случайные события доступны только в групповых чатах. Создайте групповой чат с друзьями, добавьте в него бота и планируйте просмотр кино вместе 👥",
                    show_alert=True
                )
            except Exception as e:
                logger.error(f"[SETTINGS] Ошибка при ответе на callback для random_events_locked: {e}")
            return
        
        # Проверяем random_events для личных чатов ПЕРЕД общим answer_callback_query
        if action == "random_events":
            # Проверяем, что это не личный чат
            if is_private:
                bot.answer_callback_query(
                    call.id,
                    "Раздел доступен только в групповых чатах. Создайте групповой чат с друзьями, добавьте в него бота и планируйте просмотр кино вместе 👥",
                    show_alert=True
                )
                return
            
            # Для групповых чатов вызываем answer_callback_query
            bot.answer_callback_query(call.id)
            
            # Показываем настройку случайных событий
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                # Получаем ID бота динамически
                bot_id = bot.get_me().id
                
                # Вычисляем timestamp за последние 30 дней (точно как в random_events.py)
                threshold_time = (datetime.now(PLANS_TZ) - timedelta(days=30)).isoformat()
                
                # Считаем количество активных участников (исключая бота)
                with db_lock:
                    cursor_local.execute('''
                        SELECT COUNT(DISTINCT user_id) AS count
                        FROM stats 
                        WHERE chat_id = %s 
                        AND timestamp >= %s
                        AND user_id != %s
                    ''', (chat_id, threshold_time, bot_id))
                    count_row = cursor_local.fetchone()
                    active_count = count_row.get('count') if isinstance(count_row, dict) else (count_row[0] if count_row else 0)
                
                # Получаем текущий статус случайных событий из базы (работает и с dict, и с tuple)
                with db_lock:
                    cursor_local.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'random_events_enabled'", (chat_id,))
                    row = cursor_local.fetchone()
                    
                    if row is None:
                        is_enabled = True  # по умолчанию включено, если записи нет
                    else:
                        # Универсальное получение значения
                        value = row.get('value') if isinstance(row, dict) else (row[0] if row else 'true')
                        is_enabled = str(value).lower() == 'true'

                markup = InlineKeyboardMarkup(row_width=1)
                if is_enabled:
                    markup.add(InlineKeyboardButton("❌ Выключить", callback_data="settings:random_events:disable"))
                else:
                    markup.add(InlineKeyboardButton("✅ Включить", callback_data="settings:random_events:enable"))
                markup.add(InlineKeyboardButton("📋 Пример события с участником", callback_data="settings:random_events:example:with_user"))
                markup.add(InlineKeyboardButton("📋 Пример события без участника", callback_data="settings:random_events:example:without_user"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:back"))
                
                status_text = "включены" if is_enabled else "выключены"
                bot.edit_message_text(
                    f"🎲 <b>Случайные события</b>\n\n"
                    f"Текущий статус: <b>{status_text}</b>\n\n"
                    f"Случайные события включают:\n"
                    f"• Выбор броском кубика случайного участника для выбора фильма (раз в 2 недели)\n"
                    f"• Выбор случайного участника для выбора фильма ботом",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            return
        
        # Обработчик для примеров событий (должен быть ДО общего answer_callback_query)
        if action.startswith("random_events:example:"):
            # Отправка примера случайного события
            example_type = action.split(":")[-1]  # with_user или without_user
            
            # Проверяем, что это групповой чат
            try:
                chat_info = bot.get_chat(chat_id)
                if chat_info.type == 'private':
                    bot.answer_callback_query(call.id, "Примеры событий работают только в групповых чатах", show_alert=True)
                    return
            except Exception as e:
                logger.warning(f"[RANDOM EVENTS EXAMPLE] Не удалось получить информацию о чате {chat_id}: {e}")
                bot.answer_callback_query(call.id, "Ошибка при отправке примера", show_alert=True)
                return
            
            bot.answer_callback_query(call.id, "Отправляю пример события...")
            
            if example_type == "with_user":
                # Пример события с участником (выбор случайного участника)
                current_bot_id = None
                try:
                    bot_info = bot.get_me()
                    current_bot_id = bot_info.id
                except Exception as e:
                    logger.warning(f"Не удалось получить информацию о боте: {e}")

                conn_local_ex = get_db_connection()
                cursor_local_ex = get_db_cursor()
                try:
                    with db_lock:
                        if current_bot_id:
                            cursor_local_ex.execute('''
                                SELECT DISTINCT user_id, username 
                                FROM stats 
                                WHERE chat_id = %s 
                                AND user_id != %s
                                LIMIT 10
                            ''', (chat_id, current_bot_id))
                        else:
                            cursor_local_ex.execute('''
                                SELECT DISTINCT user_id, username 
                                FROM stats 
                                WHERE chat_id = %s 
                                LIMIT 10
                            ''', (chat_id,))
                        participants = cursor_local_ex.fetchall()
                finally:
                    try:
                        cursor_local_ex.close()
                    except:
                        pass
                    try:
                        conn_local_ex.close()
                    except:
                        pass
                
                if current_bot_id:
                    filtered_participants = []
                    for p in participants:
                        p_user_id = p.get('user_id') if isinstance(p, dict) else p[0]
                        if p_user_id != current_bot_id:
                            filtered_participants.append(p)
                    participants = filtered_participants
                
                if participants:
                    participant = random.choice(participants)
                    p_user_id = participant.get('user_id') if isinstance(participant, dict) else participant[0]
                    username = participant.get('username') if isinstance(participant, dict) else participant[1]
                    
                    if username:
                        user_name = f"@{username}"
                    else:
                        try:
                            user_info = bot.get_chat_member(chat_id, p_user_id)
                            user_name = user_info.user.first_name or "участник"
                        except:
                            user_name = "участник"
                else:
                    user_name = "участник"
                
                markup = InlineKeyboardMarkup(row_width=1)
                # Используем p_user_id для ограничения доступа к кнопке только выбранному участнику
                markup.add(InlineKeyboardButton("🎲 Найти фильм", callback_data=f"rand_final:go:{p_user_id}"))
                markup.add(InlineKeyboardButton("❌ Отменить такие уведомления", callback_data="reminder:disable:random_events"))
                markup.add(InlineKeyboardButton("❌ Закрыть", callback_data="random_event:close"))
                
                text = "🔮 Вас посетил дух выбора случайного фильма!\n\n"
                text += f"Он выбрал <b>{user_name}</b> для выбора фильма для вашей компании."
                
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
            else:
                # Пример события без участника (игра в кубик) - используем общую функцию
                from moviebot.utils.random_events import send_dice_game_event
                success = send_dice_game_event(chat_id, skip_checks=True)  # skip_checks=True для примера
                if not success:
                    bot.answer_callback_query(call.id, "Ошибка при отправке примера события", show_alert=True)
                    return
            
            return
        
        # Для остальных действий вызываем обычный answer_callback_query в начале
        bot.answer_callback_query(call.id)
        
        if action == "notifications":
            # Проверяем доступ к настройкам напоминаний
            if not has_notifications_access(chat_id, user_id):
                bot.answer_callback_query(
                    call.id,
                    "🔒 Функционал можно подключить через /payment",
                    show_alert=True
                )
                return
            
            # Показываем настройки времени напоминаний
            notify_settings = get_notification_settings(chat_id)
            
            separate = notify_settings.get('separate_weekdays', 'true') == 'true'
            
            markup = InlineKeyboardMarkup(row_width=1)
            # Обновляем текст кнопки в зависимости от статуса
            separate_button_text = "✅ Разделять будни/выходные" if separate else "❌ Разделять будни/выходные"
            markup.add(InlineKeyboardButton(separate_button_text, callback_data="settings:notify:separate_toggle"))
            markup.add(InlineKeyboardButton("🏠 Домашний просмотр", callback_data="settings:notify:home"))
            markup.add(InlineKeyboardButton("🎬 Просмотр в кино", callback_data="settings:notify:cinema"))
            markup.add(InlineKeyboardButton("🎫 Билеты на сеанс", callback_data="settings:notify:tickets"))
            markup.add(InlineKeyboardButton("📋 Регулярные напоминания", callback_data="settings:notify:regular_reminders"))
            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:back"))
            
            separate_text = "✅ Включено" if separate else "❌ Выключено"
            home_weekday = f"{notify_settings.get('home_weekday_hour', 19):02d}:{notify_settings.get('home_weekday_minute', 0):02d}"
            home_weekend = f"{notify_settings.get('home_weekend_hour', 9):02d}:{notify_settings.get('home_weekend_minute', 0):02d}"
            cinema_weekday = f"{notify_settings.get('cinema_weekday_hour', 9):02d}:{notify_settings.get('cinema_weekday_minute', 0):02d}"
            cinema_weekend = f"{notify_settings.get('cinema_weekend_hour', 9):02d}:{notify_settings.get('cinema_weekend_minute', 0):02d}"
            ticket_minutes = notify_settings.get('ticket_before_minutes', 10)
            
            if ticket_minutes == -1:
                ticket_text = "Не присылать отдельно"
            elif ticket_minutes == 0:
                ticket_text = "Вместе с уведомлением"
            else:
                ticket_text = f"За {ticket_minutes} минут"
            
            text = f"⏰ <b>Настройки напоминаний</b>\n\n"
            text += f"📅 Разделение будни/выходные: <b>{separate_text}</b>\n\n"
            text += f"🏠 <b>Домашний просмотр:</b>\n"
            if separate:
                text += f"   Будни: <b>{home_weekday}</b>\n"
                text += f"   Выходные: <b>{home_weekend}</b>\n"
            else:
                text += f"   Время: <b>{home_weekday}</b>\n"
            text += f"\n🎬 <b>Просмотр в кино:</b>\n"
            if separate:
                text += f"   Будни: <b>{cinema_weekday}</b>\n"
                text += f"   Выходные: <b>{cinema_weekend}</b>\n"
            else:
                text += f"   Время: <b>{cinema_weekday}</b>\n"
            text += f"\n🎫 <b>Билеты на сеанс:</b> <b>{ticket_text}</b>"
            
            bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
            return
        
        if action == "import":
            # Проверяем доступ к импорту базы из Кинопоиска
            if not has_recommendations_access(chat_id, user_id):
                bot.answer_callback_query(
                    call.id,
                    "📥 Импорт базы из Кинопоиска доступен с подпиской 🎯 Рекомендации или 📦 Все режимы. Подключите подписку через /payment",
                    show_alert=True
                )
                return
            
            # Импорт базы из Кинопоиска
            user_import_state[user_id] = {
                'step': 'waiting_user_id',
                'kp_user_id': None,
                'count': None,
                'prompt_message_id': call.message.message_id
            }
            msg = bot.edit_message_text(
                f"📥 <b>Импорт базы из Кинопоиска</b>\n\n"
                f"Отправьте ID пользователя Кинопоиска или ссылку на профиль.\n\n"
                f"Примеры:\n"
                f"• <code>1931396</code>\n"
                f"• <code>https://www.kinopoisk.ru/user/1931396</code>",
                call.message.chat.id,
                call.message.message_id,
                parse_mode='HTML'
            )
            # Сохраняем ID сообщения в состоянии для проверки ответного сообщения
            if msg:
                user_import_state[user_id]['prompt_message_id'] = msg.message_id
            else:
                # Если edit не удался, используем исходный message_id
                user_import_state[user_id]['prompt_message_id'] = call.message.message_id
            logger.info(f"[SETTINGS] Импорт базы - состояние установлено для user_id={user_id}, prompt_message_id={user_import_state[user_id]['prompt_message_id']}")
            return
        
        if action.startswith("random_events:"):
            # Включение/выключение случайных событий
            sub_action = action.split(":", 1)[1]
            new_value = 'true' if sub_action == 'enable' else 'false'
            
            conn_local_re = get_db_connection()
            cursor_local_re = get_db_cursor()
            try:
                with db_lock:
                    cursor_local_re.execute('''
                        INSERT INTO settings (chat_id, key, value)
                        VALUES (%s, 'random_events_enabled', %s)
                        ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                    ''', (chat_id, new_value))
                    conn_local_re.commit()
            finally:
                try:
                    cursor_local_re.close()
                except:
                    pass
                try:
                    conn_local_re.close()
                except:
                    pass
            
            status_text = "включены" if new_value == 'true' else "выключены"
            bot.answer_callback_query(call.id, f"Случайные события {status_text}")
            
            # Обновляем сообщение
            markup = InlineKeyboardMarkup(row_width=1)
            if new_value == 'true':
                markup.add(InlineKeyboardButton("❌ Выключить", callback_data="settings:random_events:disable"))
            else:
                markup.add(InlineKeyboardButton("✅ Включить", callback_data="settings:random_events:enable"))
            markup.add(InlineKeyboardButton("📋 Пример события с участником", callback_data="settings:random_events:example:with_user"))
            markup.add(InlineKeyboardButton("📋 Пример события без участника", callback_data="settings:random_events:example:without_user"))
            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:back"))
            
            bot.edit_message_text(
                f"🎲 <b>Случайные события</b>\n\n"
                f"Текущий статус: <b>{status_text}</b>\n\n"
                f"Случайные события включают:\n"
                f"• Выбор броском кубика случайного участника для выбора фильма (раз в 2 недели)\n"
                f"• Выбор случайного участника для выбора фильма ботом",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
            return
        
        if action == "timezone":
            # Показываем выбор часового пояса
            current_tz = get_user_timezone(user_id)
            # Отображаемое имя для текущего пояса
            if not current_tz:
                current_tz_name = "не установлен"
            else:
                tz_zone = current_tz.zone
                tz_display_map = {
                    'Europe/Moscow': "Москва",
                    'Europe/Belgrade': "Сербия",
                    'Europe/Samara': "Самара (+1 МСК)",
                    'Asia/Yekaterinburg': "Екатеринбург (+2 МСК)",
                    'Asia/Novosibirsk': "Новосибирск (+4 МСК)",
                    'Asia/Yakutsk': "Якутск (+6 МСК)",
                    'Asia/Vladivostok': "Владивосток (+7 МСК)",
                    'Asia/Magadan': "Магадан (+8 МСК)",
                }
                current_tz_name = tz_display_map.get(tz_zone, tz_zone)
            
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("🇷🇺 Москва (Europe/Moscow)", callback_data="timezone:Moscow"))
            markup.add(InlineKeyboardButton("🇷🇸 Сербия (Europe/Belgrade)", callback_data="timezone:Serbia"))
            markup.add(InlineKeyboardButton("🇷🇺 Самара (+1 МСК)", callback_data="timezone:Samara"))
            markup.add(InlineKeyboardButton("🇷🇺 Екатеринбург (+2 МСК)", callback_data="timezone:Yekaterinburg"))
            markup.add(InlineKeyboardButton("🇷🇺 Новосибирск (+4 МСК)", callback_data="timezone:Novosibirsk"))
            markup.add(InlineKeyboardButton("🇷🇺 Якутск (+6 МСК)", callback_data="timezone:Yakutsk"))
            markup.add(InlineKeyboardButton("🇷🇺 Владивосток (+7 МСК)", callback_data="timezone:Vladivostok"))
            markup.add(InlineKeyboardButton("🇷🇺 Магадан (+8 МСК)", callback_data="timezone:Magadan"))
            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:back"))
            
            bot.edit_message_text(
                f"🕐 <b>Выбор часового пояса</b>\n\n"
                f"Текущий: <b>{current_tz_name}</b>\n\n"
                f"Выберите часовой пояс. Все время будет отображаться и планироваться в выбранном часовом поясе.",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
            return
        
        if action == "edit":
            # Вызываем команду /edit
            logger.info(f"[SETTINGS CALLBACK] Обработка action=edit для user_id={user_id}, chat_id={chat_id}")
            
            # Отвечаем на callback сразу
            bot.answer_callback_query(call.id)
            
            try:
                from moviebot.bot.handlers.settings.edit import edit_command
                logger.info(f"[SETTINGS CALLBACK] edit_command успешно импортирован")
            except ImportError as e:
                logger.error(f"[SETTINGS CALLBACK] Ошибка импорта edit_command: {e}", exc_info=True)
                try:
                    bot.send_message(chat_id, "❌ Ошибка: не удалось загрузить команду /edit. Попробуйте вызвать её напрямую: /edit")
                except:
                    pass
                return
            except Exception as e:
                logger.error(f"[SETTINGS CALLBACK] Неожиданная ошибка при импорте edit_command: {e}", exc_info=True)
                try:
                    bot.send_message(chat_id, "❌ Ошибка при загрузке команды. Попробуйте вызвать /edit напрямую")
                except:
                    pass
                return
            
            # Создаем полноценный fake_message с всеми необходимыми атрибутами
            # НЕ удаляем сообщение до вызова команды, чтобы reply_to работал
            class FakeMessage:
                def __init__(self, call):
                    self.message_id = call.message.message_id
                    self.from_user = call.from_user
                    self.chat = call.message.chat
                    self.date = call.message.date
                    self.text = '/edit'
                    # Сохраняем оригинальное сообщение для reply_to
                    self._original_message = call.message
            
            try:
                # Устанавливаем флаг from_settings перед вызовом edit_command
                from moviebot.states import user_edit_state
                if user_id not in user_edit_state:
                    user_edit_state[user_id] = {}
                user_edit_state[user_id]['from_settings'] = True
                
                fake_message = FakeMessage(call)
                logger.info(f"[SETTINGS CALLBACK] Вызов edit_command для user_id={user_id}, chat_id={chat_id}")
                edit_command(fake_message)
                logger.info(f"[SETTINGS CALLBACK] edit_command успешно выполнен")
                
                # Удаляем сообщение после успешного выполнения команды
                try:
                    bot.delete_message(chat_id, call.message.message_id)
                    logger.info(f"[SETTINGS CALLBACK] Сообщение {call.message.message_id} удалено после выполнения команды")
                except Exception as e:
                    logger.warning(f"[SETTINGS CALLBACK] Не удалось удалить сообщение после выполнения: {e}")
            except Exception as e:
                logger.error(f"[SETTINGS CALLBACK] Ошибка при вызове edit_command: {e}", exc_info=True)
                # Удаляем сообщение даже при ошибке
                try:
                    bot.delete_message(chat_id, call.message.message_id)
                except:
                    pass
                try:
                    bot.send_message(chat_id, "❌ Произошла ошибка при выполнении команды /edit. Попробуйте вызвать её напрямую: /edit")
                except:
                    pass
            return
        
        if action == "clean":
            # Вызываем команду /clean
            logger.info(f"[SETTINGS CALLBACK] Обработка action=clean для user_id={user_id}, chat_id={chat_id}")
            
            # Отвечаем на callback сразу
            bot.answer_callback_query(call.id)
            
            try:
                from moviebot.bot.handlers.settings.clean import clean_command
                logger.info(f"[SETTINGS CALLBACK] clean_command успешно импортирован")
            except ImportError as e:
                logger.error(f"[SETTINGS CALLBACK] Ошибка импорта clean_command: {e}", exc_info=True)
                try:
                    bot.send_message(chat_id, "❌ Ошибка: не удалось загрузить команду /clean. Попробуйте вызвать её напрямую: /clean")
                except:
                    pass
                return
            except Exception as e:
                logger.error(f"[SETTINGS CALLBACK] Неожиданная ошибка при импорте clean_command: {e}", exc_info=True)
                try:
                    bot.send_message(chat_id, "❌ Ошибка при загрузке команды. Попробуйте вызвать /clean напрямую")
                except:
                    pass
                return
            
            # Создаем полноценный fake_message с всеми необходимыми атрибутами
            # НЕ удаляем сообщение до вызова команды, чтобы reply_to работал
            class FakeMessage:
                def __init__(self, call):
                    self.message_id = call.message.message_id
                    self.from_user = call.from_user
                    self.chat = call.message.chat
                    self.date = call.message.date
                    self.text = '/clean'
                    # Сохраняем оригинальное сообщение для reply_to
                    self._original_message = call.message
            
            try:
                fake_message = FakeMessage(call)
                logger.info(f"[SETTINGS CALLBACK] Вызов clean_command для user_id={user_id}, chat_id={chat_id}")
                clean_command(fake_message)
                logger.info(f"[SETTINGS CALLBACK] clean_command успешно выполнен")
                
                # Удаляем сообщение после успешного выполнения команды
                try:
                    bot.delete_message(chat_id, call.message.message_id)
                    logger.info(f"[SETTINGS CALLBACK] Сообщение {call.message.message_id} удалено после выполнения команды")
                except Exception as e:
                    logger.warning(f"[SETTINGS CALLBACK] Не удалось удалить сообщение после выполнения: {e}")
            except Exception as e:
                logger.error(f"[SETTINGS CALLBACK] Ошибка при вызове clean_command: {e}", exc_info=True)
                # Удаляем сообщение даже при ошибке
                try:
                    bot.delete_message(chat_id, call.message.message_id)
                except:
                    pass
                try:
                    bot.send_message(chat_id, "❌ Произошла ошибка при выполнении команды /clean. Попробуйте вызвать её напрямую: /clean")
                except:
                    pass
            return
        
        if action == "join":
            # Вызываем команду /join
            logger.info(f"[SETTINGS CALLBACK] Обработка action=join для user_id={user_id}, chat_id={chat_id}")
            
            # Отвечаем на callback сразу
            bot.answer_callback_query(call.id)
            
            try:
                from moviebot.bot.handlers.settings.join import join_command
                logger.info(f"[SETTINGS CALLBACK] join_command успешно импортирован")
            except ImportError as e:
                logger.error(f"[SETTINGS CALLBACK] Ошибка импорта join_command: {e}", exc_info=True)
                try:
                    bot.send_message(chat_id, "❌ Ошибка: не удалось загрузить команду /join. Попробуйте вызвать её напрямую: /join")
                except:
                    pass
                return
            except Exception as e:
                logger.error(f"[SETTINGS CALLBACK] Неожиданная ошибка при импорте join_command: {e}", exc_info=True)
                try:
                    bot.send_message(chat_id, "❌ Ошибка при загрузке команды. Попробуйте вызвать /join напрямую")
                except:
                    pass
                return
            
            # Создаем полноценный fake_message с всеми необходимыми атрибутами
            # НЕ удаляем сообщение до вызова команды, чтобы reply_to работал
            class FakeMessage:
                def __init__(self, call):
                    self.message_id = call.message.message_id
                    self.from_user = call.from_user
                    self.chat = call.message.chat
                    self.date = call.message.date
                    self.text = '/join'
                    # Сохраняем оригинальное сообщение для reply_to
                    self._original_message = call.message
            
            try:
                fake_message = FakeMessage(call)
                logger.info(f"[SETTINGS CALLBACK] Вызов join_command для user_id={user_id}, chat_id={chat_id}")
                join_command(fake_message)
                logger.info(f"[SETTINGS CALLBACK] join_command успешно выполнен")
                
                # Удаляем сообщение после успешного выполнения команды
                try:
                    bot.delete_message(chat_id, call.message.message_id)
                    logger.info(f"[SETTINGS CALLBACK] Сообщение {call.message.message_id} удалено после выполнения команды")
                except Exception as e:
                    logger.warning(f"[SETTINGS CALLBACK] Не удалось удалить сообщение после выполнения: {e}")
            except Exception as e:
                logger.error(f"[SETTINGS CALLBACK] Ошибка при вызове join_command: {e}", exc_info=True)
                # Удаляем сообщение даже при ошибке
                try:
                    bot.delete_message(chat_id, call.message.message_id)
                except:
                    pass
                try:
                    bot.send_message(chat_id, "❌ Произошла ошибка при выполнении команды /join. Попробуйте вызвать её напрямую: /join")
                except:
                    pass
            return
        
        if action == "back":
            # Возврат к главному меню settings
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("🕐 Выбрать часовой пояс", callback_data="settings:timezone"))
            
            # Проверяем доступ к настройкам напоминаний
            if has_notifications_access(chat_id, user_id):
                markup.add(InlineKeyboardButton("⏰ Настройки напоминаний", callback_data="settings:notifications"))
            else:
                markup.add(InlineKeyboardButton("🔒 Настройки напоминаний", callback_data="settings:notifications_locked"))
            
            # Проверяем доступ к импорту базы
            if has_recommendations_access(chat_id, user_id):
                markup.add(InlineKeyboardButton("📥 Импорт базы из Кинопоиска", callback_data="settings:import"))
            else:
                markup.add(InlineKeyboardButton("🔒 Импорт базы из Кинопоиска", callback_data="settings:import_locked"))
            
            # Проверяем, является ли чат личным (случайные события доступны только в группах)
            if is_private:
                markup.add(InlineKeyboardButton("🔒 Случайные события", callback_data="settings:random_events_locked"))
            else:
                markup.add(InlineKeyboardButton("🎲 Случайные события", callback_data="settings:random_events"))
            markup.add(InlineKeyboardButton("✏️ Редактировать записи", callback_data="settings:edit"))
            markup.add(InlineKeyboardButton("🗑️ Очистка базы", callback_data="settings:clean"))
            markup.add(InlineKeyboardButton("👥 Участие", callback_data="settings:join"))
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            
            bot.edit_message_text(
                f"⚙️ <b>Настройки</b>\n\n"
                f"Выберите, что хотите настроить:",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )

            # Обновляем информацию о сообщении settings
            if call.message.message_id in settings_messages:
                settings_messages[call.message.message_id]['action'] = action
            else:
                settings_messages[call.message.message_id] = {
                    'user_id': user_id,
                    'action': action,
                    'chat_id': call.message.chat.id
                }
            logger.info(f"[SETTINGS] Пользователь {user_id} выбрал режим: {action}")
            return
        
        # Обработка подменю настроек напоминаний
        if action.startswith("notify:"):
            sub_action = action.split(":", 1)[1]
            notify_settings = get_notification_settings(chat_id)
            
            if sub_action == "separate_toggle":
                # Переключение разделения будни/выходные
                current = notify_settings.get('separate_weekdays', 'true')
                new_value = 'false' if current == 'true' else 'true'
                set_notification_setting(chat_id, 'notify_separate_weekdays', new_value)
                bot.answer_callback_query(call.id, f"Разделение будни/выходные {'включено' if new_value == 'true' else 'выключено'}")
                # Возвращаемся к меню настроек напоминаний
                action = "notifications"
                # Рекурсивно вызываем обработчик для обновления меню
                call.data = f"settings:{action}"
                handle_settings_callback(call)
                return
            
            elif sub_action == "tickets":
                # Настройка времени отправки билетов
                ticket_minutes = notify_settings.get('ticket_before_minutes', 10)
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("⏰ За 10 минут", callback_data="settings:notify:tickets:10"))
                markup.add(InlineKeyboardButton("⏰ За 30 минут", callback_data="settings:notify:tickets:30"))
                markup.add(InlineKeyboardButton("⏰ За 1 час", callback_data="settings:notify:tickets:60"))
                markup.add(InlineKeyboardButton("📨 Вместе с уведомлением", callback_data="settings:notify:tickets:0"))
                markup.add(InlineKeyboardButton("❌ Не присылать отдельно", callback_data="settings:notify:tickets:-1"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:notifications"))
                
                if ticket_minutes == -1:
                    ticket_text = "Не присылать отдельно"
                elif ticket_minutes == 0:
                    ticket_text = "Вместе с уведомлением"
                else:
                    ticket_text = f"За {ticket_minutes} минут"
                
                text = f"🎫 <b>Настройка отправки билетов на сеанс</b>\n\n"
                text += f"Текущая настройка: <b>{ticket_text}</b>\n\n"
                text += f"Выберите, когда присылать билеты:"
                
                bot.edit_message_text(
                    text,
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                return
            
            elif sub_action.startswith("tickets:"):
                # Сохранение настройки времени отправки билетов
                minutes = int(sub_action.split(":", 1)[1])
                set_notification_setting(chat_id, 'ticket_before_minutes', minutes)
                
                if minutes == -1:
                    ticket_text = "Не присылать отдельно"
                elif minutes == 0:
                    ticket_text = "Вместе с уведомлением"
                else:
                    ticket_text = f"За {minutes} минут"
                
                bot.answer_callback_query(call.id, f"Билеты: {ticket_text}")
                # Возвращаемся к меню настроек напоминаний
                call.data = "settings:notifications"
                handle_settings_callback(call)
                return
            
            elif sub_action == "home":
                # Настройка времени для домашнего просмотра
                separate = notify_settings.get('separate_weekdays', 'true') == 'true'
                markup = InlineKeyboardMarkup(row_width=1)
                if separate:
                    markup.add(InlineKeyboardButton("📅 Будни", callback_data="settings:notify:home:weekday"))
                    markup.add(InlineKeyboardButton("🌴 Выходные", callback_data="settings:notify:home:weekend"))
                else:
                    markup.add(InlineKeyboardButton("⏰ Установить время", callback_data="settings:notify:home:time"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:notifications"))
                
                home_weekday = f"{notify_settings.get('home_weekday_hour', 19):02d}:{notify_settings.get('home_weekday_minute', 0):02d}"
                home_weekend = f"{notify_settings.get('home_weekend_hour', 9):02d}:{notify_settings.get('home_weekend_minute', 0):02d}"
                
                text = f"🏠 <b>Настройка времени напоминаний для домашнего просмотра</b>\n\n"
                if separate:
                    text += f"📅 Будни: <b>{home_weekday}</b>\n"
                    text += f"🌴 Выходные: <b>{home_weekend}</b>\n"
                else:
                    text += f"⏰ Время: <b>{home_weekday}</b>\n"
                text += f"\nОтправьте время в формате ЧЧ:ММ (например, 19:00 или 09:00)"
                
                bot.edit_message_text(
                    text,
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                # Сохраняем состояние для обработки ввода времени
                if user_id not in user_settings_state:
                    user_settings_state[user_id] = {}
                user_settings_state[user_id]['waiting_notify_time'] = 'home'
                user_settings_state[user_id]['notify_separate'] = separate
                user_settings_state[user_id]['prompt_message_id'] = call.message.message_id
                return
            
            elif sub_action.startswith("home:"):
                # Обработка выбора будни/выходные для домашнего просмотра
                time_type = sub_action.split(":", 1)[1]  # "weekday" или "weekend"
                if user_id not in user_settings_state:
                    user_settings_state[user_id] = {}
                user_settings_state[user_id]['waiting_notify_time'] = f'home_{time_type}'
                user_settings_state[user_id]['notify_separate'] = True
                user_settings_state[user_id]['prompt_message_id'] = call.message.message_id
                
                bot.answer_callback_query(call.id)
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:notify:home"))
                bot.edit_message_text(
                    f"🏠 <b>Настройка времени для домашнего просмотра</b>\n\n"
                    f"📅 {'Будни' if time_type == 'weekday' else 'Выходные'}\n\n"
                    f"Отправьте время в формате ЧЧ:ММ (например, 19:00 или 09:00)",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                return
            
            elif sub_action == "cinema":
                # Настройка времени для кино
                separate = notify_settings.get('separate_weekdays', 'true') == 'true'
                markup = InlineKeyboardMarkup(row_width=1)
                if separate:
                    markup.add(InlineKeyboardButton("📅 Будни", callback_data="settings:notify:cinema:weekday"))
                    markup.add(InlineKeyboardButton("🌴 Выходные", callback_data="settings:notify:cinema:weekend"))
                else:
                    markup.add(InlineKeyboardButton("⏰ Установить время", callback_data="settings:notify:cinema:time"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:notifications"))
                
                cinema_weekday = f"{notify_settings.get('cinema_weekday_hour', 9):02d}:{notify_settings.get('cinema_weekday_minute', 0):02d}"
                cinema_weekend = f"{notify_settings.get('cinema_weekend_hour', 9):02d}:{notify_settings.get('cinema_weekend_minute', 0):02d}"
                
                text = f"🎬 <b>Настройка времени напоминаний для просмотра в кино</b>\n\n"
                if separate:
                    text += f"📅 Будни: <b>{cinema_weekday}</b>\n"
                    text += f"🌴 Выходные: <b>{cinema_weekend}</b>\n"
                else:
                    text += f"⏰ Время: <b>{cinema_weekday}</b>\n"
                text += f"\nОтправьте время в формате ЧЧ:ММ (например, 09:00)"
                
                bot.edit_message_text(
                    text,
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                if user_id not in user_settings_state:
                    user_settings_state[user_id] = {}
                user_settings_state[user_id]['waiting_notify_time'] = 'cinema'
                user_settings_state[user_id]['notify_separate'] = separate
                user_settings_state[user_id]['prompt_message_id'] = call.message.message_id
                return
            
            elif sub_action.startswith("cinema:"):
                # Обработка выбора будни/выходные для кино
                time_type = sub_action.split(":", 1)[1]  # "weekday" или "weekend"
                if user_id not in user_settings_state:
                    user_settings_state[user_id] = {}
                user_settings_state[user_id]['waiting_notify_time'] = f'cinema_{time_type}'
                user_settings_state[user_id]['notify_separate'] = True
                user_settings_state[user_id]['prompt_message_id'] = call.message.message_id
                
                bot.answer_callback_query(call.id)
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:notify:cinema"))
                bot.edit_message_text(
                    f"🎬 <b>Настройка времени для просмотра в кино</b>\n\n"
                    f"📅 {'Будни' if time_type == 'weekday' else 'Выходные'}\n\n"
                    f"Отправьте время в формате ЧЧ:ММ (например, 09:00)",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                return
            
            elif sub_action == "regular_reminders":
                # Показываем меню регулярных напоминаний
                conn_local_rem = get_db_connection()
                cursor_local_rem = get_db_cursor()
                with db_lock:
                    # Проверяем статус каждого напоминания
                    cursor_local_rem.execute("SELECT key, value FROM settings WHERE chat_id = %s AND key IN ('reminder_weekend_films_disabled', 'reminder_cinema_premieres_disabled', 'random_events_enabled')", (chat_id,))
                    reminder_rows = cursor_local_rem.fetchall()
                
                reminders_status = {}
                for row in reminder_rows:
                    key = row.get('key') if isinstance(row, dict) else row[0]
                    value = row.get('value') if isinstance(row, dict) else row[1]
                    reminders_status[key] = value
                
                markup = InlineKeyboardMarkup(row_width=1)
                
                # Напоминание о фильмах на выходных
                weekend_films_disabled = reminders_status.get('reminder_weekend_films_disabled', 'false') == 'true'
                if weekend_films_disabled:
                    markup.add(InlineKeyboardButton("⏰ Включить: Фильмы на выходных", callback_data="reminder:enable:weekend_films"))
                else:
                    markup.add(InlineKeyboardButton("❌ Отменить: Фильмы на выходных", callback_data="reminder:disable:weekend_films"))
                
                # Напоминание о премьерах в кино
                cinema_premieres_disabled = reminders_status.get('reminder_cinema_premieres_disabled', 'false') == 'true'
                if cinema_premieres_disabled:
                    markup.add(InlineKeyboardButton("⏰ Включить: Премьеры в кино", callback_data="reminder:enable:cinema_premieres"))
                else:
                    markup.add(InlineKeyboardButton("❌ Отменить: Премьеры в кино", callback_data="reminder:disable:cinema_premieres"))
                
                # Случайные события (все сразу)
                random_events_enabled = reminders_status.get('random_events_enabled', 'true') == 'true'
                if not random_events_enabled:
                    markup.add(InlineKeyboardButton("⏰ Включить: Случайные события", callback_data="reminder:enable:random_events"))
                else:
                    markup.add(InlineKeyboardButton("❌ Отменить: Случайные события", callback_data="reminder:disable:random_events"))
                
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:notifications"))
                
                text = "📋 <b>Регулярные напоминания</b>\n\n"
                text += "Управление регулярными напоминаниями бота:\n\n"
                text += "• <b>Фильмы на выходных</b> — напоминание каждую субботу, если нет планов\n"
                text += "• <b>Премьеры в кино</b> — напоминание о премьерах, если давно не добавляли фильмы в кино\n"
                text += "• <b>Случайные события</b> — все случайные события (выбор участника, игра в кубик и т.д.)"
                
                bot.edit_message_text(
                    text,
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                return
        
        logger.warning(f"[SETTINGS CALLBACK] Необработанное действие: {action}, callback_data={call.data}")
        try:
            bot.answer_callback_query(call.id, f"Действие '{action}' будет реализовано позже", show_alert=True)
        except:
            pass
    except Exception as e:
        logger.error(f"[SETTINGS CALLBACK] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass
    finally:
        logger.info(f"[SETTINGS CALLBACK] ===== КОНЕЦ ОБРАБОТКИ =====")


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("reminder:"))
def handle_reminder_callback(call):
    """Обработчик callback для включения/отключения напоминаний"""
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        action_parts = call.data.split(":")
        if len(action_parts) < 3:
            bot.answer_callback_query(call.id, "Ошибка обработки", show_alert=True)
            return
        
        reminder_action = action_parts[1]  # "disable" или "enable"
        reminder_type = action_parts[2]  # "weekend_films", "cinema_premieres", "random_events", "unwatched_films"
        
        logger.info(f"[REMINDER CALLBACK] action={reminder_action}, type={reminder_type}, chat_id={chat_id}, user_id={user_id}")
        
        # Определяем ключ в БД для каждого типа напоминания
        if reminder_type == "weekend_films":
            setting_key = 'reminder_weekend_films_disabled'
            new_value = 'true' if reminder_action == 'disable' else 'false'
            success_text = "Фильмы на выходных отменены" if reminder_action == 'disable' else "Фильмы на выходных включены"
        elif reminder_type == "cinema_premieres":
            setting_key = 'reminder_cinema_premieres_disabled'
            new_value = 'true' if reminder_action == 'disable' else 'false'
            success_text = "Премьеры в кино отменены" if reminder_action == 'disable' else "Премьеры в кино включены"
        elif reminder_type == "random_events":
            setting_key = 'random_events_enabled'
            new_value = 'true' if reminder_action == 'enable' else 'false'
            success_text = "Случайные события включены" if reminder_action == 'enable' else "Случайные события отменены"
        elif reminder_type == "unwatched_films":
            setting_key = 'reminder_unwatched_films_disabled'
            new_value = 'true' if reminder_action == 'disable' else 'false'
            success_text = "Уведомления о непросмотренных фильмах отменены" if reminder_action == 'disable' else "Уведомления о непросмотренных фильмах включены"
        else:
            bot.answer_callback_query(call.id, "Неизвестный тип напоминания", show_alert=True)
            return
        
        # Сохраняем настройку в БД
        conn_local_rem2 = get_db_connection()
        cursor_local_rem2 = get_db_cursor()
        try:
            with db_lock:
                cursor_local_rem2.execute("""
                    INSERT INTO settings (chat_id, key, value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                """, (chat_id, setting_key, new_value))
                conn_local_rem2.commit()
            
            logger.info(f"[REMINDER CALLBACK] Настройка сохранена: {setting_key}={new_value}")
            
            # Обновляем меню регулярных напоминаний
            with db_lock:
                cursor_local_rem2.execute("SELECT key, value FROM settings WHERE chat_id = %s AND key IN ('reminder_weekend_films_disabled', 'reminder_cinema_premieres_disabled', 'random_events_enabled', 'reminder_unwatched_films_disabled')", (chat_id,))
                reminder_rows = cursor_local_rem2.fetchall()
        finally:
            try:
                cursor_local_rem2.close()
            except:
                pass
            try:
                conn_local_rem2.close()
            except:
                pass
            
            reminders_status = {}
            for row in reminder_rows:
                key = row.get('key') if isinstance(row, dict) else row[0]
                value = row.get('value') if isinstance(row, dict) else row[1]
                reminders_status[key] = value
        
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Напоминание о фильмах на выходных
        weekend_films_disabled = reminders_status.get('reminder_weekend_films_disabled', 'false') == 'true'
        if weekend_films_disabled:
            markup.add(InlineKeyboardButton("⏰ Включить: Фильмы на выходных", callback_data="reminder:enable:weekend_films"))
        else:
            markup.add(InlineKeyboardButton("❌ Отменить: Фильмы на выходных", callback_data="reminder:disable:weekend_films"))
        
        # Напоминание о премьерах в кино
        cinema_premieres_disabled = reminders_status.get('reminder_cinema_premieres_disabled', 'false') == 'true'
        if cinema_premieres_disabled:
            markup.add(InlineKeyboardButton("⏰ Включить: Премьеры в кино", callback_data="reminder:enable:cinema_premieres"))
        else:
            markup.add(InlineKeyboardButton("❌ Отменить: Премьеры в кино", callback_data="reminder:disable:cinema_premieres"))
        
        # Случайные события (все сразу)
        random_events_enabled = reminders_status.get('random_events_enabled', 'true') == 'true'
        if not random_events_enabled:
            markup.add(InlineKeyboardButton("⏰ Включить: Случайные события", callback_data="reminder:enable:random_events"))
        else:
            markup.add(InlineKeyboardButton("❌ Отменить: Случайные события", callback_data="reminder:disable:random_events"))
        
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:notifications"))
        
        text = "📋 <b>Регулярные напоминания</b>\n\n"
        text += "Управление регулярными напоминаниями бота:\n\n"
        text += "• <b>Фильмы на выходных</b> — напоминание каждую субботу, если нет планов\n"
        text += "• <b>Премьеры в кино</b> — напоминание о премьерах, если давно не добавляли фильмы в кино\n"
        text += "• <b>Случайные события</b> — все случайные события (выбор участника, игра в кубик и т.д.)"
        
        try:
            bot.edit_message_text(
                text,
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
            bot.answer_callback_query(call.id, success_text)
        except Exception as e:
            logger.error(f"[REMINDER CALLBACK] Ошибка обновления сообщения: {e}", exc_info=True)
            bot.answer_callback_query(call.id, success_text)
    except Exception as e:
        logger.error(f"[REMINDER CALLBACK] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("timezone:"))
def handle_timezone_callback(call):
    """Обработчик выбора часового пояса"""
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        timezone_name = call.data.split(":", 1)[1]  # "Moscow", "Serbia", "Samara", "Yekaterinburg", "Novosibirsk"

        # Карта идентификаторов к отображаемым именам и pytz-таймзонам
        tz_info = {
            "Moscow": ("Москва", "Europe/Moscow"),
            "Serbia": ("Сербия", "Europe/Belgrade"),
            "Samara": ("Самара (+1 МСК)", "Europe/Samara"),
            "Yekaterinburg": ("Екатеринбург (+2 МСК)", "Asia/Yekaterinburg"),
            "Novosibirsk": ("Новосибирск (+4 МСК)", "Asia/Novosibirsk"),
            "Yakutsk": ("Якутск (+6 МСК)", "Asia/Yakutsk"),
            "Vladivostok": ("Владивосток (+7 МСК)", "Asia/Vladivostok"),
            "Magadan": ("Магадан (+8 МСК)", "Asia/Magadan"),
        }

        if timezone_name not in tz_info:
            logger.error(f"[TIMEZONE] Неизвестный идентификатор часового пояса: {timezone_name}")
            return

        if set_user_timezone(user_id, timezone_name):
            tz_display, tz_code = tz_info[timezone_name]
            tz_obj = pytz.timezone(tz_code)
            current_time = datetime.now(tz_obj).strftime('%H:%M')
            
            bot.edit_message_text(
                f"✅ Часовой пояс установлен: <b>{tz_display}</b>\n\n"
                f"Текущее время: <b>{current_time}</b>\n\n"
                f"Все время будет отображаться и планироваться в часовом поясе {tz_display}.\n"
                f"Часовой пояс будет автоматически обновляться при путешествиях.",
                call.message.chat.id,
                call.message.message_id,
                parse_mode='HTML'
            )
            logger.info(f"Часовой пояс установлен для user_id={user_id}: {timezone_name}")
            
            # Проверяем, есть ли сохраненный текст для продолжения планирования
            from moviebot.states import user_plan_state, user_view_film_state
            # Проверяем user_view_film_state
            if user_id in user_view_film_state:
                state = user_view_film_state[user_id]
                chat_id_from_state = state.get('chat_id', chat_id)
                
                logger.info(f"[VIEW FILM REPLY] Пользователь {user_id} в user_view_film_state, chat_id={chat_id_from_state}")
                
                # Обработка ответного сообщения для просмотра фильма
                from moviebot.bot.handlers.list import handle_view_film_reply_internal
                # Создаем fake message для handle_view_film_reply_internal
                class FakeMessage:
                    def __init__(self, call, state):
                        self.message_id = call.message.message_id
                        self.from_user = call.from_user
                        self.chat = type('Chat', (), {'id': state.get('chat_id', call.message.chat.id)})()
                        self.date = call.message.date
                        self.text = state.get('pending_text', '')
                        self.reply_to_message = None
                
                fake_message = FakeMessage(call, state)
                handle_view_film_reply_internal(fake_message, state)
                return
            
            # Проверяем user_plan_state
            if user_id in user_plan_state:
                state = user_plan_state[user_id]
                # КРИТИЧЕСКИЙ ФИКС: Продолжаем планирование, если есть все необходимые данные
                # (pending_text не обязателен, главное - link, plan_type, pending_plan_dt)
                link = state.get('link')
                plan_type = state.get('plan_type')  # ИСПРАВЛЕНО: было 'type'
                pending_plan_dt = state.get('pending_plan_dt')
                pending_message_date_utc = state.get('pending_message_date_utc')
                chat_id_from_state = state.get('chat_id', chat_id)
                pending_text = state.get('pending_text')
                
                if link and plan_type and pending_plan_dt:
                    logger.info(f"[TIMEZONE CALLBACK] Продолжаем планирование: link={link}, plan_type={plan_type}, pending_plan_dt={pending_plan_dt}")
                    # Импортируем process_plan из handlers/plan
                    from moviebot.bot.handlers.plan import process_plan
                    # Вызываем process_plan с сохраненными данными
                    result = process_plan(bot, user_id, chat_id_from_state, link, plan_type, pending_plan_dt, pending_message_date_utc)
                    if result:
                        # Очищаем сохраненные данные
                        if 'pending_text' in state:
                            del state['pending_text']
                        if 'pending_plan_dt' in state:
                            del state['pending_plan_dt']
                        if 'pending_message_date_utc' in state:
                            del state['pending_message_date_utc']
                        del user_plan_state[user_id]
                        logger.info(f"[TIMEZONE CALLBACK] Планирование успешно завершено")
                    else:
                        logger.warning(f"[TIMEZONE CALLBACK] Ошибка при продолжении планирования")
                elif link and plan_type:
                    # Если есть link и plan_type, но нет pending_plan_dt - устанавливаем step=3 для продолжения планирования
                    logger.info(f"[TIMEZONE CALLBACK] Устанавливаем step=3 для продолжения планирования: link={link}, plan_type={plan_type}")
                    state['step'] = 3
                    state['chat_id'] = chat_id_from_state
                    user_plan_state[user_id] = state
                    # Отправляем промпт для ввода времени/даты
                    # bot уже импортирован в начале файла, не нужно импортировать снова
                    prompt_msg = bot.send_message(
                        chat_id_from_state,
                        f"Когда планируете смотреть {'дома' if plan_type == 'home' else 'в кино'}?\n\n"
                        f"Примеры: сегодня 21:00, завтра 19:30, пт 18:45, 15 января 20:00"
                    )
                    state['prompt_message_id'] = prompt_msg.message_id
                    user_plan_state[user_id] = state
                    logger.info(f"[TIMEZONE CALLBACK] Промпт отправлен для продолжения планирования, prompt_message_id={prompt_msg.message_id}")
                else:
                    logger.warning(f"[TIMEZONE CALLBACK] Недостаточно данных для продолжения планирования: link={link}, plan_type={plan_type}, pending_plan_dt={pending_plan_dt}")
        else:
            bot.answer_callback_query(call.id, "Ошибка сохранения часового пояса", show_alert=True)
    except Exception as e:
        logger.error(f"[TIMEZONE CALLBACK] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data == "cancel_action")
def settings_cancel_action_callback(call):
    """Обработчик cancel_action для настроек - возвращает в настройки"""
    logger.info(f"[SETTINGS CANCEL ACTION] ===== START: callback_id={call.id}, user_id={call.from_user.id}")
    try:
        from moviebot.bot.bot_init import safe_answer_callback_query
        safe_answer_callback_query(bot, call.id)
        
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # Очищаем все состояния
        from moviebot.states import (
            user_ticket_state, user_search_state, user_import_state,
            user_edit_state, user_settings_state, user_plan_state,
            user_clean_state, user_promo_state, user_promo_admin_state,
            user_cancel_subscription_state, user_refund_state,
            user_unsubscribe_state, user_add_admin_state, user_view_film_state
        )
        
        states_to_clear = [
            user_ticket_state, user_search_state, user_import_state,
            user_edit_state, user_settings_state, user_plan_state,
            user_clean_state, user_promo_state, user_promo_admin_state,
            user_cancel_subscription_state, user_refund_state,
            user_unsubscribe_state, user_add_admin_state, user_view_film_state
        ]
        
        for state_dict in states_to_clear:
            if user_id in state_dict:
                del state_dict[user_id]
        
        # Возвращаемся в настройки
        from moviebot.bot.handlers.settings_main import settings_command
        class FakeMessage:
            def __init__(self, call):
                self.from_user = call.from_user
                self.chat = call.message.chat
                self.text = '/settings'
        
        fake_message = FakeMessage(call)
        settings_command(fake_message)
        
        # Удаляем старое сообщение
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
    except Exception as e:
        logger.error(f"[SETTINGS CANCEL ACTION] Ошибка: {e}", exc_info=True)
        try:
            from moviebot.bot.bot_init import safe_answer_callback_query
            safe_answer_callback_query(bot, call.id, "❌ Ошибка", show_alert=True)
        except:
            pass


