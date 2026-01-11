"""
Модуль для работы со случайными событиями (игра с кубиком и т.д.)
"""
import logging
from datetime import datetime, timedelta
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from telebot.apihelper import ApiTelegramException  # <<< ВАЖНО: импортируем исключение

from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.config import PLANS_TZ
from moviebot.states import dice_game_state
from moviebot.bot.bot_init import bot

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()
plans_tz = PLANS_TZ


def _get_random_events_enabled(chat_id):
    """Вспомогательная функция для проверки включенности случайных событий"""
    with db_lock:
        cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'random_events_enabled'", (chat_id,))
        row = cursor.fetchone()
        if row:
            value = row.get('value') if isinstance(row, dict) else row[0]
            return value == 'true'
    return True  # По умолчанию включено


def _mark_event_sent(chat_id, event_type):
    """Вспомогательная функция для отметки отправленного события"""
    now = datetime.now(plans_tz)
    today = now.date()
    with db_lock:
        cursor.execute("""
            INSERT INTO event_notifications (chat_id, event_type, sent_date)
            VALUES (%s, %s, %s)
            ON CONFLICT (chat_id, event_type, sent_date) DO NOTHING
        """, (chat_id, event_type, today))
        conn.commit()


def send_dice_game_event(chat_id, skip_checks=False):
    """
    Общая функция для отправки события игры в кубик
    
    Args:
        chat_id: ID чата
        skip_checks: Если True, пропускает проверки на активных участников и время (для примеров из настроек)
    
    Returns:
        bool: True если событие успешно отправлено, False иначе
    """
    try:
        now = datetime.now(plans_tz)
        
        # Проверяем, что это групповой чат (не личный)
        try:
            chat_info = bot.get_chat(chat_id)
            if chat_info.type == 'private':
                logger.warning(f"[DICE GAME] Чат {chat_id} является личным, пропускаем")
                return False
        except Exception as e:
            logger.warning(f"[DICE GAME] Не удалось получить информацию о чате {chat_id}: {e}")
            return False
        
        # Проверяем, включены ли случайные события (если не пропускаем проверки)
        if not skip_checks:
            if not _get_random_events_enabled(chat_id):
                logger.info(f"[DICE GAME] Случайные события выключены для чата {chat_id}")
                return False
            
            # Проверяем, когда последний раз запускали игру
            with db_lock:
                cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'last_dice_game_date'", (chat_id,))
                last_date_row = cursor.fetchone()
            
            if last_date_row:
                last_date_str = last_date_row.get('value') if isinstance(last_date_row, dict) else last_date_row[0]
                try:
                    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
                    days_passed = (now.date() - last_date).days
                    if days_passed < 14:
                        logger.info(f"[DICE GAME] Для чата {chat_id} прошло только {days_passed} дней с последнего события (нужно 14)")
                        return False
                except Exception as e:
                    logger.warning(f"[DICE GAME] Ошибка при парсинге last_dice_game_date: {e}")
        
        # Проверяем количество активных участников (если не пропускаем проверки)
        if not skip_checks:
            try:
                chat_members_count = bot.get_chat_member_count(chat_id)
                total_participants = max(1, chat_members_count - 1)
            except Exception as e:
                logger.warning(f"[DICE GAME] Не удалось получить количество участников чата {chat_id}: {e}")
                return False
            
            threshold_time = (now - timedelta(days=30)).isoformat()
            with db_lock:
                bot_id = bot.get_me().id
                cursor.execute('''
                    SELECT COUNT(DISTINCT user_id) AS count
                    FROM stats 
                    WHERE chat_id = %s 
                    AND timestamp >= %s
                    AND user_id != %s
                ''', (chat_id, threshold_time, bot_id))
                row = cursor.fetchone()
                active_participants = row.get("count") if isinstance(row, dict) else (row[0] if row else 0)
            
            required_participants = int(total_participants * 0.65)
            if active_participants < required_participants:
                logger.info(f"[DICE GAME] Для чата {chat_id} недостаточно активных участников ({active_participants} из {required_participants})")
                return False
        
        # Отправляем сообщение
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("❌ Отменить такие уведомления", callback_data="reminder:disable:random_events"))
        markup.add(InlineKeyboardButton("❌ Закрыть", callback_data="random_event:close"))
        
        text = "🔮 Вас посетил дух выбора случайного фильма!\n\n"
        text += "Испытайте удачу и определите, кто выберет фильм для вашей компании.\n\n"
        text += "Ниже бот бросит тестовый кубик, вы можете на него нажать, чтобы тоже сделать бросок.\n\n"
        text += "Также, вы можете просто отправить эмодзи кубика в чат, бросок будет засчитан.\n\n"
        text += "📝 Итоги будут подведены через 10 минут, даже если не все участники сделали бросок"
        
        current_chat_id = chat_id
        
        try:
            msg = bot.send_message(
                chat_id=current_chat_id,
                text=text,
                reply_markup=markup,
                parse_mode='HTML'
            )
        except ApiTelegramException as e:
            if e.error_code == 400 and 'upgraded to a supergroup chat' in str(e.description).lower():
                try:
                    new_chat_id = e.result_json['parameters']['migrate_to_chat_id']
                    logger.info(f"[DICE GAME] Чат {chat_id} мигрировал в супергруппу {new_chat_id}. Отправляем туда.")
                    
                    msg = bot.send_message(
                        chat_id=new_chat_id,
                        text=text,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                    
                    current_chat_id = new_chat_id
                except Exception as e2:
                    logger.error(f"[DICE GAME] Не удалось отправить сообщение даже в новый чат {new_chat_id}: {e2}", exc_info=True)
                    return False
            else:
                logger.error(f"[DICE GAME] Ошибка Telegram API при отправке в чат {chat_id}: {e}", exc_info=True)
                return False
        except Exception as e:
            logger.error(f"[DICE GAME] Непредвиденная ошибка при отправке в чат {chat_id}: {e}", exc_info=True)
            return False
        
        # Сохраняем состояние игры
        dice_game_state[current_chat_id] = {
            'participants': {},
            'message_id': msg.message_id,
            'start_time': now,
            'dice_messages': {}
        }
        
        # Автоматически бросаем кубик от имени бота после отправки сообщения
        try:
            bot_dice_msg = bot.send_dice(current_chat_id, emoji='🎲')
            logger.info(f"[DICE GAME] Бот автоматически бросил кубик в чате {current_chat_id}, message_id={bot_dice_msg.message_id if bot_dice_msg else None}")
        except Exception as dice_e:
            logger.error(f"[DICE GAME] Ошибка при автоматическом броске кубика: {dice_e}", exc_info=True)
        
        # Отмечаем, что событие отправлено (если не пропускаем проверки)
        if not skip_checks:
            _mark_event_sent(current_chat_id, 'random_event')
            
            # Сохраняем дату последнего запуска
            with db_lock:
                cursor.execute('''
                    INSERT INTO settings (chat_id, key, value)
                    VALUES (%s, 'last_dice_game_date', %s)
                    ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                ''', (current_chat_id, now.date().isoformat()))
                conn.commit()
        
        logger.info(f"[DICE GAME] Успешно отправлено событие игры в кубик для чата {current_chat_id}")
        return True
        
    except Exception as e:
        logger.error(f"[DICE GAME] Критическая ошибка в send_dice_game_event: {e}", exc_info=True)
        return False


def update_dice_game_message(chat_id, game_state, message_id, bot_id=None):
    """
    Обновляет сообщение с игрой в кубик, показывая результаты и количество оставшихся участников
    bot_id - ID бота для исключения из подсчета участников
    """
    try:
        # Получаем список всех активных участников (исключая бота)
        with db_lock:
            if bot_id:
                cursor.execute('''
                    SELECT DISTINCT user_id 
                    FROM stats 
                    WHERE chat_id = %s 
                    AND timestamp >= %s
                    AND user_id != %s
                ''', (chat_id, (datetime.now(plans_tz) - timedelta(days=30)).isoformat(), bot_id))
            else:
                cursor.execute('''
                    SELECT DISTINCT user_id 
                    FROM stats 
                    WHERE chat_id = %s 
                    AND timestamp >= %s
                ''', (chat_id, (datetime.now(plans_tz) - timedelta(days=30)).isoformat()))
            all_participants = [row[0] if not isinstance(row, dict) else row.get('user_id') for row in cursor.fetchall()]
        
        # Формируем текст с результатами
        text = "🔮 Вас посетил дух выбора случайного фильма!\n\n"
        text += "Испытайте удачу и определите, кто выберет фильм для вашей компании.\n\n"
        text += "Отправьте эмодзи кубика 🎲 в чат, чтобы сделать бросок.\n\n"
        
        # Показываем результаты бросков
        participants_with_results = []
        participants_without_results = []
        
        for uid, p in game_state.get('participants', {}).items():
            username = p.get('username', f"user_{uid}")
            if 'value' in p and p['value'] is not None:
                participants_with_results.append((username, p['value']))
            else:
                participants_without_results.append(uid)
        
        if participants_with_results:
            text += "<b>Результаты бросков:</b>\n"
            for username, value in sorted(participants_with_results, key=lambda x: x[1], reverse=True):
                text += f"• {username}: <b>{value}</b>\n"
            text += "\n"
        
        # Подсчёт оставшихся
        participants_who_threw = set(game_state.get('participants', {}).keys())
        remaining_participants = [uid for uid in all_participants if uid not in participants_who_threw]
        remaining_count = len(remaining_participants)
        
        participants_with_values_dict = {uid: p['value'] for uid, p in game_state.get('participants', {}).items() if 'value' in p and p['value'] is not None}
        
        # Проверяем, истекло ли время игры (10 минут)
        start_time = game_state.get('start_time')
        if start_time:
            if isinstance(start_time, str):
                # Парсим строку ISO формата
                try:
                    # Пробуем использовать fromisoformat (Python 3.7+)
                    if hasattr(datetime, 'fromisoformat'):
                        if start_time.endswith('Z'):
                            start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                        else:
                            start_time = datetime.fromisoformat(start_time)
                    else:
                        # Fallback для старых версий Python
                        from dateutil.parser import parse
                        start_time = parse(start_time)
                    if start_time.tzinfo is None:
                        start_time = plans_tz.localize(start_time)
                    elif start_time.tzinfo != plans_tz:
                        start_time = start_time.astimezone(plans_tz)
                except Exception as e:
                    logger.warning(f"[DICE GAME] Ошибка при парсинге start_time: {e}, используем текущее время")
                    start_time = datetime.now(plans_tz)
            elif start_time.tzinfo is None:
                start_time = plans_tz.localize(start_time)
            elapsed_seconds = (datetime.now(plans_tz) - start_time).total_seconds()
            game_expired = elapsed_seconds >= 600  # 10 минут = 600 секунд
        else:
            game_expired = False
        
        is_example_or_small_group = len(game_state.get('participants', {})) >= 2
        
        if is_example_or_small_group:
            all_threw = True
            all_have_results = len(participants_with_values_dict) == len(game_state.get('participants', {})) and len(participants_with_values_dict) >= 2
        else:
            all_threw = remaining_count == 0
            all_have_results = len(participants_without_results) == 0 and len(participants_with_results) > 0
        
        # Если время игры истекло и есть результаты, определяем победителя (тот, кто выбросил больше)
        if game_expired and participants_with_values_dict:
            max_value = max(participants_with_values_dict.values())
            winners = [uid for uid, val in participants_with_values_dict.items() if val == max_value]
            
            if len(winners) == 1:
                winner_id = winners[0]
                winner_info = game_state['participants'][winner_id]
                winner_name = winner_info.get('username', 'участник')
                
                try:
                    user_info = bot.get_chat_member(chat_id, winner_id)
                    user_display = user_info.user.first_name or winner_name
                except:
                    user_display = winner_name if winner_name and not winner_name.startswith('user_') else "участник"
                
                text += f"⏰ <b>Время вышло!</b>\n\n"
                text += f"🏆 <b>Победитель: {user_display}</b> (выбросил {max_value})\n\n"
                text += f"🎬 {user_display} выбирает фильм для вашей компании!\n"
                
                # Отправка сообщения победителю
                winner_mention = f"@{winner_info.get('username')}" if winner_info.get('username') else user_display
                
                markup_winner = InlineKeyboardMarkup(row_width=1)
                markup_winner.add(InlineKeyboardButton("🎲 Рандом", callback_data="start_menu:random"))
                markup_winner.add(InlineKeyboardButton("🔍 Поиск фильмов и сериалов", callback_data="start_menu:search"))
                markup_winner.add(InlineKeyboardButton("📅 Премьеры", callback_data="start_menu:premieres"))
                
                bot.send_message(
                    chat_id,
                    f"<b>{winner_mention}</b>, поздравляю! Приглашаю выбрать фильм для просмотра:",
                    reply_markup=markup_winner,
                    parse_mode='HTML'
                )
                
                if chat_id in dice_game_state:
                    del dice_game_state[chat_id]
                return
            elif len(winners) > 1:
                winner_names = []
                for winner_id in winners:
                    winner_info = game_state['participants'][winner_id]
                    winner_name = winner_info.get('username', 'участник')
                    try:
                        user_info = bot.get_chat_member(chat_id, winner_id)
                        user_display = user_info.user.first_name or winner_name
                    except:
                        user_display = winner_name if not winner_name.startswith('user_') else "участник"
                    winner_names.append(user_display)
                text += f"⏰ <b>Время вышло!</b>\n\n"
                text += f"🤝 <b>Ничья!</b> У {len(winners)} участников выпало {max_value}:\n"
                for name in winner_names:
                    text += f"• {name}\n"
                text += "\n🎲 Перекидываем кубик для определения победителя!\n"
        
        elif all_threw and all_have_results and participants_with_values_dict:
            max_value = max(participants_with_values_dict.values())
            winners = [uid for uid, val in participants_with_values_dict.items() if val == max_value]
            
            if len(winners) == 1:
                winner_id = winners[0]
                winner_info = game_state['participants'][winner_id]
                winner_name = winner_info.get('username', 'участник')
                
                try:
                    user_info = bot.get_chat_member(chat_id, winner_id)
                    user_display = user_info.user.first_name or winner_name
                except:
                    user_display = winner_name if winner_name and not winner_name.startswith('user_') else "участник"
                
                text += f"🏆 <b>Победитель: {user_display}</b> (выбросил {max_value})\n\n"
                text += f"🎬 {user_display} выбирает фильм для вашей компании!\n"
                
                # Отправка сообщения победителю
                winner_mention = f"@{winner_info.get('username')}" if winner_info.get('username') else user_display
                
                markup_winner = InlineKeyboardMarkup(row_width=1)
                markup_winner.add(InlineKeyboardButton("🎲 Рандом", callback_data="start_menu:random"))
                markup_winner.add(InlineKeyboardButton("🔍 Поиск фильмов и сериалов", callback_data="start_menu:search"))
                markup_winner.add(InlineKeyboardButton("📅 Премьеры", callback_data="start_menu:premieres"))
                
                bot.send_message(
                    chat_id,
                    f"<b>{winner_mention}</b>, поздравляю! Приглашаю выбрать фильм для просмотра:",
                    reply_markup=markup_winner,
                    parse_mode='HTML'
                )
                
                if chat_id in dice_game_state:
                    del dice_game_state[chat_id]
                    
            elif len(winners) > 1:
                winner_names = []
                for winner_id in winners:
                    winner_info = game_state['participants'][winner_id]
                    winner_name = winner_info.get('username', 'участник')
                    try:
                        user_info = bot.get_chat_member(chat_id, winner_id)
                        user_display = user_info.user.first_name or winner_name
                    except:
                        user_display = winner_name if not winner_name.startswith('user_') else "участник"
                    winner_names.append(user_display)
                text += f"🤝 <b>Ничья!</b> У {len(winners)} участников выпало {max_value}:\n"
                for name in winner_names:
                    text += f"• {name}\n"
                text += "\n🎲 Перекидываем кубик для определения победителя!\n"
                
        elif remaining_count > 0:
            text += f"⏳ Осталось бросить кубик: <b>{remaining_count}</b> участник(ов)\n\n"
        elif len(participants_without_results) > 0:
            text += f"⏳ Ожидаем результаты бросков...\n\n"
        else:
            text += "✅ Все участники бросили кубик!\n\n"
        
        # Клавиатура (убрана кнопка "Бросить кубик" - пользователи отправляют кубики сами)
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("❌ Отменить такие уведомления", callback_data="reminder:disable:random_events"))
        markup.add(InlineKeyboardButton("❌ Закрыть", callback_data="random_event:close"))
        
        # <<< КРИТИЧЕСКИЙ ФИКС: обрабатываем ошибку "message not modified" >>>
        try:
            bot.edit_message_text(
                text=text,
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
        except ApiTelegramException as e:
            if e.error_code == 400 and "message is not modified" in str(e.description).lower():
                logger.debug(f"[DICE GAME] Сообщение не изменилось — пропускаем edit (chat_id={chat_id}, message_id={message_id})")
                return
            else:
                logger.error(f"[DICE GAME] Ошибка Telegram API при edit_message_text: {e}", exc_info=True)
                raise
        except Exception as e:
            logger.error(f"[DICE GAME] Неизвестная ошибка при обновлении сообщения: {e}", exc_info=True)
            raise
            
    except Exception as e:
        logger.error(f"[DICE GAME] Критическая ошибка в update_dice_game_message: {e}", exc_info=True)
        raise
