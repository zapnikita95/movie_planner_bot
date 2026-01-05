"""
Обработчики команд /stats, /total, /admin_stats
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request, get_admin_statistics
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


def register_stats_handlers(bot_instance):
    """Регистрирует обработчики команд статистики"""
    
    @bot_instance.message_handler(commands=['stats'])
    def stats_command(message):
        """Команда /stats - детальная статистика группы и участников"""
        # TODO: Извлечь из moviebot.py строки 8407-9153
        logger.info(f"[HANDLER] /stats вызван от {message.from_user.id}")
        try:
            username = message.from_user.username or f"user_{message.from_user.id}"
            log_request(message.from_user.id, username, '/stats', message.chat.id)
            logger.info(f"Команда /stats от пользователя {message.from_user.id}, chat_id={message.chat.id}")
            chat_id = message.chat.id
            
            with db_lock:
                # Получаем всех участников из разных источников: stats, ratings, watched_movies, plans
                all_users = {}
                
                # Из stats (команды)
                cursor.execute('''
                    SELECT 
                        user_id,
                        username,
                        COUNT(*) as command_count,
                        MAX(timestamp) as last_activity
                    FROM stats
                    WHERE chat_id = %s AND user_id IS NOT NULL
                    GROUP BY user_id, username
                ''', (chat_id,))
                for row in cursor.fetchall():
                    user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                    username = row.get('username') if isinstance(row, dict) else row[1]
                    command_count = row.get('command_count') if isinstance(row, dict) else row[2]
                    last_activity = row.get('last_activity') if isinstance(row, dict) else row[3]
                    all_users[user_id] = {
                        'username': username,
                        'command_count': command_count,
                        'last_activity': last_activity
                    }
                
                # Из ratings (оценки)
                cursor.execute('''
                    SELECT DISTINCT user_id
                    FROM ratings
                    WHERE chat_id = %s AND user_id IS NOT NULL
                ''', (chat_id,))
                for row in cursor.fetchall():
                    user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                    if user_id not in all_users:
                        all_users[user_id] = {
                            'username': None,
                            'command_count': 0,
                            'last_activity': None
                        }
                
                # TODO: Добавить остальную логику из moviebot.py строки 8579-9153
                # Это очень большая функция, нужно скопировать весь код
                
                # Временная заглушка
                text = f"📊 <b>Статистика группы</b>\n\n"
                text += f"Участников: {len(all_users)}\n\n"
                text += "<i>Полная статистика будет доступна после завершения рефакторинга</i>"
                
                bot_instance.reply_to(message, text, parse_mode='HTML')
                logger.info(f"✅ Ответ на /stats отправлен пользователю {message.from_user.id}")
        except Exception as e:
            logger.error(f"❌ Ошибка в /stats: {e}", exc_info=True)
            try:
                bot_instance.reply_to(message, "Произошла ошибка при обработке команды /stats")
            except:
                pass

    @bot_instance.message_handler(commands=['total'])
    def total_stats(message):
        """Команда /total - статистика: фильмы, жанры, режиссёры, актёры и оценки"""
        # TODO: Извлечь из moviebot.py строки 9188-9387
        logger.info(f"[HANDLER] /total вызван от {message.from_user.id}")
        try:
            username = message.from_user.username or f"user_{message.from_user.id}"
            log_request(message.from_user.id, username, '/total', message.chat.id)
            logger.info(f"Команда /total от пользователя {message.from_user.id}")
            chat_id = message.chat.id
            
            with db_lock:
                # Исключаем фильмы, добавленные только через импорт
                cursor.execute('''
                    SELECT COUNT(*) as count FROM movies m
                    WHERE m.chat_id = %s
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
                ''', (chat_id,))
                total_row = cursor.fetchone()
                total = total_row.get('count') if isinstance(total_row, dict) else (total_row[0] if total_row and len(total_row) > 0 else 0)
                
                cursor.execute('''
                    SELECT COUNT(*) as count FROM movies m
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
                ''', (chat_id,))
                watched_row = cursor.fetchone()
                watched = watched_row.get('count') if isinstance(watched_row, dict) else (watched_row[0] if watched_row and len(watched_row) > 0 else 0)
                unwatched = total - watched
                
                # Если нет данных, отправляем сообщение
                if total == 0:
                    bot_instance.reply_to(message, "📊 Нет данных о вашей статистике.\n\nОцените первый фильм, чтобы статистика начала собираться.")
                    return
                
                # TODO: Добавить полную логику из moviebot.py строки 9353-9487
                # Это очень большая функция, нужно скопировать весь код
                
                # Временная заглушка
                text = f"📊 <b>Статистика кино-группы</b>\n\n"
                text += f"🎬 Всего фильмов: <b>{total}</b>\n"
                text += f"✅ Просмотрено: <b>{watched}</b>\n"
                text += f"⏳ Ждёт просмотра: <b>{unwatched}</b>\n"
                text += "\n<i>Полная статистика будет доступна после завершения рефакторинга</i>"
                
                bot_instance.reply_to(message, text, parse_mode='HTML')
                logger.info(f"✅ Ответ на /total отправлен пользователю {message.from_user.id}")
        except Exception as e:
            logger.error(f"❌ Ошибка в /total: {e}", exc_info=True)
            try:
                bot_instance.reply_to(message, "Произошла ошибка при обработке команды /total")
            except:
                pass

    @bot_instance.message_handler(commands=['admin_stats'])
    def admin_stats_command(message):
        """Команда /admin_stats - статистика для администратора"""
        # ID создателя бота
        CREATOR_ID = 301810276
        
        if message.from_user.id != CREATOR_ID:
            bot_instance.reply_to(message, "❌ У вас нет доступа к этой команде.")
            return
        
        try:
            logger.info(f"[HANDLER] /admin_stats вызван от {message.from_user.id}")
            stats = get_admin_statistics()
            
            if 'error' in stats:
                bot_instance.reply_to(message, f"❌ Ошибка получения статистики: {stats['error']}")
                return
            
            # Формируем сообщение со статистикой
            text = "📊 <b>СТАТИСТИКА БОТА</b>\n\n"
            
            text += "👥 <b>Пользователи:</b>\n"
            text += f"   • Активных за 30 дней: {stats.get('active_users_30d', 0)}\n"
            text += f"   • Всего пользователей: {stats.get('total_users', 0)}\n"
            text += f"   • Новых за день: {stats.get('new_users_day', 0)}\n"
            text += f"   • Новых за неделю: {stats.get('new_users_week', 0)}\n"
            text += f"   • Платных пользователей: {stats.get('paid_users', 0)}\n\n"
            
            text += "👥 <b>Группы:</b>\n"
            text += f"   • Активных за 30 дней: {stats.get('active_groups_30d', 0)}\n"
            text += f"   • Всего групп: {stats.get('total_groups', 0)}\n"
            text += f"   • Платных групп: {stats.get('paid_groups', 0)}\n\n"
            
            text += "🌐 <b>Запросы к API Кинопоиска:</b>\n"
            text += f"   • За день: {stats.get('kp_api_requests_day', 0)}\n"
            text += f"   • За неделю: {stats.get('kp_api_requests_week', 0)}\n"
            text += f"   • За месяц: {stats.get('kp_api_requests_month', 0)}\n"
            text += f"   • Всего: {stats.get('kp_api_requests_total', 0)}\n\n"
            
            text += "📝 <b>Запросы пользователей:</b>\n"
            text += f"   • За день: {stats.get('user_requests_day', 0)}\n"
            text += f"   • За неделю: {stats.get('user_requests_week', 0)}\n"
            text += f"   • За месяц: {stats.get('user_requests_month', 0)}\n\n"
            
            text += "💳 <b>Подписки:</b>\n"
            text += f"   • Новых за день: {stats.get('new_subscriptions_day', 0)}\n"
            text += f"   • Новых за неделю: {stats.get('new_subscriptions_week', 0)}\n"
            text += f"   • Отписавшихся за неделю: {stats.get('cancelled_subscriptions_week', 0)}\n\n"
            
            text += "🎬 <b>Контент:</b>\n"
            text += f"   • Всего фильмов: {stats.get('total_movies', 0)}\n"
            text += f"   • Всего планов: {stats.get('total_plans', 0)}\n"
            text += f"   • Всего оценок: {stats.get('total_ratings', 0)}\n\n"
            
            # Топ команд за день
            top_commands_day = stats.get('top_commands_day', [])
            if top_commands_day:
                text += "🔥 <b>Топ команд за день:</b>\n"
                for i, cmd_row in enumerate(top_commands_day[:5], 1):
                    if isinstance(cmd_row, dict):
                        cmd = cmd_row.get('command_or_action', '')
                        count = cmd_row.get('count', 0)
                    else:
                        cmd = cmd_row[0] if len(cmd_row) > 0 else ''
                        count = cmd_row[1] if len(cmd_row) > 1 else 0
                    text += f"   {i}. {cmd}: {count}\n"
                text += "\n"
            
            # Топ команд за неделю
            top_commands_week = stats.get('top_commands_week', [])
            if top_commands_week:
                text += "📈 <b>Топ команд за неделю:</b>\n"
                for i, cmd_row in enumerate(top_commands_week[:5], 1):
                    if isinstance(cmd_row, dict):
                        cmd = cmd_row.get('command_or_action', '')
                        count = cmd_row.get('count', 0)
                    else:
                        cmd = cmd_row[0] if len(cmd_row) > 0 else ''
                        count = cmd_row[1] if len(cmd_row) > 1 else 0
                    text += f"   {i}. {cmd}: {count}\n"
            
            bot_instance.reply_to(message, text, parse_mode='HTML')
            
        except Exception as e:
            logger.error(f"Ошибка в admin_stats_command: {e}", exc_info=True)
            bot_instance.reply_to(message, f"❌ Ошибка получения статистики: {e}")

    @bot_instance.message_handler(commands=['refundstars', 'refund_stars'])
    def refundstars_command(message):
        """Команда для возврата звезд по ID операции (только для создателя)"""
        # ID создателя бота
        CREATOR_ID = 301810276
        
        if message.from_user.id != CREATOR_ID:
            bot_instance.reply_to(message, "❌ У вас нет доступа к этой команде.")
            return
        
        try:
            logger.info(f"[HANDLER] /refundstars вызван от {message.from_user.id}")
            
            # Получаем текст команды (ID операции)
            command_text = message.text.strip()
            parts = command_text.split(maxsplit=1)
            
            if len(parts) < 2:
                bot_instance.reply_to(message, "❌ Укажите ID операции для возврата.\n\n"
                                      "Использование: /refundstars <ID_операции>\n\n"
                                      "Пример: /refundstars stxwe_iXQAPRqkiZSjm9JxEiO0Ke03gNqoupstFOak10sj3ZSSeHbT2_3MukFRW4kGE-YBSssodFt05T9Szh1-N2m_FgDCvAAPloyRiqVDUp3tmzfl2I891zLP4VcZ6ul8I")
                return
            
            charge_id = parts[1].strip()
            logger.info(f"[REFUND] Запрос на возврат для charge_id: {charge_id}")
            
            # Ищем платеж в БД по telegram_payment_charge_id
            with db_lock:
                cursor.execute("""
                    SELECT payment_id, user_id, chat_id, amount, status, telegram_payment_charge_id
                    FROM payments 
                    WHERE telegram_payment_charge_id = %s
                """, (charge_id,))
                row = cursor.fetchone()
            
            if not row:
                bot_instance.reply_to(message, f"❌ Платеж с ID операции '{charge_id}' не найден в базе данных.")
                logger.warning(f"[REFUND] Платеж не найден: charge_id={charge_id}")
                return
            
            # Извлекаем данные платежа
            if isinstance(row, dict):
                payment_id = row.get('payment_id')
                user_id = row.get('user_id')
                chat_id = row.get('chat_id')
                amount = row.get('amount')
                status = row.get('status')
                stored_charge_id = row.get('telegram_payment_charge_id')
            else:
                payment_id = row[0]
                user_id = row[1]
                chat_id = row[2]
                amount = row[3]
                status = row[4]
                stored_charge_id = row[5] if len(row) > 5 else None
            
            logger.info(f"[REFUND] Найден платеж: payment_id={payment_id}, user_id={user_id}, amount={amount}, status={status}")
            
            # Проверяем, что платеж был успешным
            if status != 'succeeded':
                bot_instance.reply_to(message, f"⚠️ Платеж найден, но его статус: '{status}'. Возврат возможен только для успешных платежей.")
                return
            
            # Выполняем возврат через Telegram API
            try:
                logger.info(f"[REFUND] Выполняем возврат через Telegram API: user_id={user_id}, charge_id={charge_id}")
                
                # Используем прямой вызов API, так как pyTelegramBotAPI может не поддерживать refundStarPayment
                import requests
                from moviebot.config import TOKEN
                url = f"https://api.telegram.org/bot{TOKEN}/refundStarPayment"
                data = {
                    'user_id': user_id,
                    'telegram_payment_charge_id': charge_id
                }
                
                logger.info(f"[REFUND] Отправляем запрос: url={url}, data={data}")
                response = requests.post(url, json=data, timeout=10)
                result_data = response.json()
                
                logger.info(f"[REFUND] Ответ API: {result_data}")
                
                if result_data.get('ok'):
                    # Обновляем статус платежа в БД на 'refunded'
                    with db_lock:
                        cursor.execute("""
                            UPDATE payments 
                            SET status = 'refunded'
                            WHERE telegram_payment_charge_id = %s
                        """, (charge_id,))
                        conn.commit()
                    
                    bot_instance.reply_to(message, f"✅ Возврат выполнен успешно!\n\n"
                                          f"📋 Детали:\n"
                                          f"   • ID операции: {charge_id}\n"
                                          f"   • User ID: {user_id}\n"
                                          f"   • Сумма: {amount}₽\n"
                                          f"   • Payment ID: {payment_id}\n\n"
                                          f"Статус платежа обновлен на 'refunded'.")
                    logger.info(f"[REFUND] ✅ Возврат успешно выполнен для user_id={user_id}, charge_id={charge_id}")
                else:
                    error_description = result_data.get('description', 'Неизвестная ошибка')
                    error_code = result_data.get('error_code', 'N/A')
                    bot_instance.reply_to(message, f"❌ Ошибка возврата: {error_description}\n\n"
                                          f"Код ошибки: {error_code}\n\n"
                                          f"Возможные причины:\n"
                                          f"• Платеж уже был возвращен\n"
                                          f"• Прошло более 90 дней с момента платежа\n"
                                          f"• ID операции неверный")
                    logger.error(f"[REFUND] ❌ API вернул ошибку: {result_data}")
                    
            except Exception as e:
                logger.error(f"[REFUND] ❌ Ошибка при выполнении возврата: {e}", exc_info=True)
                bot_instance.reply_to(message, f"❌ Ошибка при выполнении возврата: {e}")
                
        except Exception as e:
            logger.error(f"Ошибка в refundstars_command: {e}", exc_info=True)
            try:
                bot_instance.reply_to(message, f"❌ Ошибка обработки команды: {e}")
            except:
                pass
