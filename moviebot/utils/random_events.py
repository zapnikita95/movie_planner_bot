"""
Модуль для работы со случайными событиями (игра с кубиком и т.д.)
"""
import logging
from datetime import datetime, timedelta
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.config import PLANS_TZ
from moviebot.states import dice_game_state
from moviebot.bot.bot_init import bot

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()
plans_tz = PLANS_TZ


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
            all_participants = [row.get('user_id') if isinstance(row, dict) else row[0] for row in cursor.fetchall()]
        
        # Формируем текст с результатами
        text = "🔮 Вас посетил дух выбора случайного фильма!\n\n"
        text += "Испытайте удачу и определите, кто выберет фильм для вашей компании.\n\n"
        
        # Показываем результаты бросков
        participants_with_results = []
        participants_without_results = []
        
        for uid, p in game_state.get('participants', {}).items():
            username = p.get('username', f"user_{uid}")
            if 'value' in p:
                participants_with_results.append((username, p['value']))
            else:
                participants_without_results.append(uid)
        
        if participants_with_results:
            text += "<b>Результаты бросков:</b>\n"
            for username, value in sorted(participants_with_results, key=lambda x: x[1], reverse=True):
                text += f"• {username}: <b>{value}</b>\n"
            text += "\n"
            logger.info(f"[DICE GAME UPDATE] Результаты бросков: {participants_with_results}")
        
        # Показываем количество оставшихся участников
        participants_who_threw = set(game_state.get('participants', {}).keys())
        remaining_participants = [uid for uid in all_participants if uid not in participants_who_threw]
        remaining_count = len(remaining_participants)
        
        # Создаем словарь значений для определения победителя
        participants_with_values_dict = {uid: p['value'] for uid, p in game_state.get('participants', {}).items() if 'value' in p}
        
        # Проверяем, все ли участники бросили и получили результаты
        # Для примера события или если в игре уже есть участники (>= 2), проверяем только их
        is_example_or_small_group = len(game_state.get('participants', {})) >= 2
        
        if is_example_or_small_group:
            # Для примера или небольшой группы: проверяем только участников, которые уже в игре
            all_threw = True  # Все участники игры уже бросили (они добавляются при броске)
            all_have_results = len(participants_with_values_dict) == len(game_state.get('participants', {})) and len(participants_with_values_dict) >= 2
        else:
            # Для реальных событий: проверяем всех участников из базы данных
            all_threw = remaining_count == 0
            all_have_results = len(participants_without_results) == 0 and len(participants_with_results) > 0
        
        if all_threw and all_have_results:
            if participants_with_values_dict:
                max_value = max(participants_with_values_dict.values())
                winners = [uid for uid, val in participants_with_values_dict.items() if val == max_value]
                
                logger.info(f"[DICE GAME UPDATE] Все бросили кубик. Результаты: {participants_with_values_dict}, максимальное значение: {max_value}, победители: {winners}")
                
                if len(winners) == 1:
                    # Есть победитель
                    winner_id = winners[0]
                    winner_info = game_state['participants'][winner_id]
                    winner_name = winner_info.get('username', 'участник')
                    
                    # Формируем имя пользователя для отображения
                    try:
                        user_info = bot.get_chat_member(chat_id, winner_id)
                        user_display = user_info.user.first_name or winner_name
                    except:
                        user_display = winner_name if winner_name and not winner_name.startswith('user_') else "участник"
                    
                    logger.info(f"[DICE GAME UPDATE] 🏆 Победитель определен: {user_display} (user_id={winner_id}, значение={max_value})")
                    
                    text += f"🏆 <b>Победитель: {user_display}</b> (выбросил {max_value})\n\n"
                    text += f"🎬 {user_display} выбирает фильм для вашей компании!\n"
                    
                    # Отправляем отдельное сообщение победителю
                    # Формируем имя с @ если есть username
                    if winner_info.get('username'):
                        winner_mention = f"@{winner_info.get('username')}"
                    else:
                        winner_mention = user_display
                    
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
                    
                    # Удаляем состояние игры
                    if chat_id in dice_game_state:
                        del dice_game_state[chat_id]
                elif len(winners) > 1:
                    # Ничья - показываем, что идет перекидывание
                    winner_names = []
                    for winner_id in winners:
                        winner_info = game_state['participants'][winner_id]
                        winner_name = winner_info.get('username', 'участник')
                        try:
                            user_info = bot.get_chat_member(chat_id, winner_id)
                            user_display = user_info.user.first_name or winner_name
                        except:
                            user_display = winner_name if winner_name and not winner_name.startswith('user_') else "участник"
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
        
        # Создаем кнопки
        markup = InlineKeyboardMarkup(row_width=1)
        # Если все бросили и есть результаты, не показываем кнопку "Бросить кубик"
        if all_threw and all_have_results:
            # Все бросили - не показываем кнопку броска
            pass
        elif remaining_count > 0 or len(participants_without_results) > 0:
            markup.add(InlineKeyboardButton("🎲 Бросить кубик", callback_data="dice_game:start"))
        markup.add(InlineKeyboardButton("❌ Отменить такие уведомления", callback_data="reminder:disable:random_events"))
        markup.add(InlineKeyboardButton("❌ Закрыть", callback_data="random_event:close"))
        
        # Обновляем сообщение
        bot.edit_message_text(
            text,
            chat_id,
            message_id,
            reply_markup=markup,
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"[DICE GAME] Ошибка при обновлении сообщения: {e}", exc_info=True)

