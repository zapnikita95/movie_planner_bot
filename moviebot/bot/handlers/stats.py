"""
Обработчики команд /stats, /total, /admin_stats
"""
import logging
from datetime import datetime
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request, get_admin_statistics
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.bot.bot_init import BOT_ID, bot as bot_instance

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


def _process_refund(message, charge_id):
    """Обрабатывает возврат звезд по charge_id"""
    try:
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
        logger.error(f"[REFUND] ❌ Ошибка при обработке возврата: {e}", exc_info=True)
        bot_instance.reply_to(message, f"❌ Ошибка при обработке возврата: {e}")


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
                
                # Из watched_movies (просмотренные фильмы)
                cursor.execute('''
                    SELECT DISTINCT user_id
                    FROM watched_movies
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
                
                # Из plans (планы)
                cursor.execute('''
                    SELECT DISTINCT user_id
                    FROM plans
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
                
                # Преобразуем в список и сортируем (исключаем бота)
                users_stats = []
                for user_id, data in all_users.items():
                    # Исключаем бота из статистики
                    if BOT_ID and user_id == BOT_ID:
                        continue
                    users_stats.append({
                        'user_id': user_id,
                        'username': data['username'],
                        'command_count': data['command_count'],
                        'last_activity': data['last_activity']
                    })
                
                # Сортируем по количеству команд и последней активности
                users_stats.sort(key=lambda x: (x['command_count'], x['last_activity'] or ''), reverse=True)
                
                # Получаем общую статистику чата (исключаем фильмы, добавленные только через импорт)
                # Фильм считается импортированным, если у него есть только импортированные оценки
                cursor.execute('''
                    SELECT COUNT(*) FROM movies m
                    WHERE m.chat_id = %s
                    AND NOT EXISTS (
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
                ''', (chat_id,))
                imported_movies_row = cursor.fetchone()
                imported_movies_count = imported_movies_row.get('count') if isinstance(imported_movies_row, dict) else (imported_movies_row[0] if imported_movies_row else 0)
                
                cursor.execute('SELECT COUNT(*) FROM movies WHERE chat_id = %s', (chat_id,))
                total_movies_row = cursor.fetchone()
                total_movies_all = total_movies_row.get('count') if isinstance(total_movies_row, dict) else (total_movies_row[0] if total_movies_row else 0)
                total_movies = total_movies_all - imported_movies_count
                
                cursor.execute('''
                    SELECT COUNT(*) FROM movies m
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
                watched_movies_row = cursor.fetchone()
                watched_movies = watched_movies_row.get('count') if isinstance(watched_movies_row, dict) else (watched_movies_row[0] if watched_movies_row else 0)
                
                # Исключаем импортированные оценки
                cursor.execute('SELECT COUNT(*) FROM ratings WHERE chat_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id,))
                total_ratings_row = cursor.fetchone()
                total_ratings = total_ratings_row.get('count') if isinstance(total_ratings_row, dict) else (total_ratings_row[0] if total_ratings_row else 0)
                
                cursor.execute('SELECT COUNT(*) FROM plans WHERE chat_id = %s', (chat_id,))
                total_plans_row = cursor.fetchone()
                total_plans = total_plans_row.get('count') if isinstance(total_plans_row, dict) else (total_plans_row[0] if total_plans_row else 0)
                
                # Статистика по сериалам (только для групповых чатов)
                watched_series_count = 0
                in_progress_series_count = 0
                is_group = chat_id < 0
                
                if is_group:
                    from datetime import datetime as dt
                    from moviebot.api.kinopoisk_api import get_seasons_data
                    
                    # Получаем все сериалы группы
                    cursor.execute('SELECT id, kp_id FROM movies WHERE chat_id = %s AND is_series = 1', (chat_id,))
                    all_series = cursor.fetchall()
                    
                    now = dt.now()
                    
                    for row in all_series:
                        if isinstance(row, dict):
                            film_id = row.get('id')
                            kp_id = row.get('kp_id')
                        else:
                            film_id = row[0]
                            kp_id = row[1]
                        
                        # Получаем данные о сезонах
                        try:
                            seasons_data = get_seasons_data(kp_id)
                            if not seasons_data:
                                continue
                        except:
                            continue
                        
                        # Проверяем, выходит ли сериал
                        is_airing = False
                        for season in seasons_data:
                            episodes = season.get('episodes', [])
                            for ep in episodes:
                                release_str = ep.get('releaseDate', '')
                                if release_str and release_str != '—':
                                    try:
                                        release_date = None
                                        for fmt in ['%Y-%m-%d', '%d.%m.%Y', '%Y-%m-%dT%H:%M:%S']:
                                            try:
                                                release_date = dt.strptime(release_str.split('T')[0], fmt)
                                                break
                                            except:
                                                continue
                                        
                                        if release_date and release_date > now:
                                            is_airing = True
                                            break
                                    except:
                                        pass
                            if is_airing:
                                break
                        
                        # Получаем всех пользователей, которые смотрели этот сериал
                        cursor.execute('''
                            SELECT DISTINCT user_id 
                            FROM series_tracking 
                            WHERE chat_id = %s AND film_id = %s AND watched = TRUE
                        ''', (chat_id, film_id))
                        users_watched = cursor.fetchall()
                        
                        if not users_watched:
                            continue
                        
                        # Для каждого пользователя проверяем статус просмотра
                        for user_row in users_watched:
                            user_id = user_row.get('user_id') if isinstance(user_row, dict) else user_row[0]
                            
                            # Получаем просмотренные эпизоды пользователя
                            cursor.execute('''
                                SELECT season_number, episode_number 
                                FROM series_tracking 
                                WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                            ''', (chat_id, film_id, user_id))
                            watched_rows = cursor.fetchall()
                            watched_set = set()
                            for w_row in watched_rows:
                                if isinstance(w_row, dict):
                                    watched_set.add((w_row.get('season_number'), w_row.get('episode_number')))
                                else:
                                    watched_set.add((w_row[0], w_row[1]))
                            
                            # Подсчитываем просмотренные и общее количество эпизодов
                            total_episodes = 0
                            watched_episodes = 0
                            all_watched = True
                            
                            for season in seasons_data:
                                episodes = season.get('episodes', [])
                                season_num = season.get('number', '')
                                for ep in episodes:
                                    total_episodes += 1
                                    ep_num = str(ep.get('episodeNumber', ''))
                                    if (season_num, ep_num) in watched_set:
                                        watched_episodes += 1
                                    else:
                                        all_watched = False
                            
                            # Если все серии просмотрены и сериал не выходит - просмотренный
                            if all_watched and total_episodes > 0 and not is_airing:
                                watched_series_count += 1
                                break  # Считаем сериал один раз для группы
                            # Если есть просмотренные, но не все - в процессе
                            elif watched_episodes > 0:
                                in_progress_series_count += 1
                                break  # Считаем сериал один раз для группы
            
            # Получаем статистику по оценкам участников
            # Для общей статистики исключаем импортированные, но для каждого пользователя считаем ВСЕ его оценки
            cursor.execute('''
                SELECT 
                    r.user_id,
                    COUNT(*) as ratings_count,
                    AVG(r.rating) as avg_rating
                FROM ratings r
                WHERE r.chat_id = %s AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                GROUP BY r.user_id
                ORDER BY ratings_count DESC
            ''', (chat_id,))
            ratings_stats = cursor.fetchall()
            ratings_by_user = {}
            for row in ratings_stats:
                user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                count = row.get('ratings_count') if isinstance(row, dict) else row[1]
                avg = row.get('avg_rating') if isinstance(row, dict) else row[2]
                ratings_by_user[user_id] = {'count': count, 'avg': avg}
            
            # Для каждого пользователя добавляем импортированные оценки (только для личной статистики)
            # Импортированные оценки НЕ учитываются в общей статистике группы, но учитываются в личной статистике пользователя
            cursor.execute('''
                SELECT 
                    r.user_id,
                    COUNT(*) as imported_count
                FROM ratings r
                WHERE r.chat_id = %s AND r.is_imported = TRUE
                GROUP BY r.user_id
            ''', (chat_id,))
            imported_stats = cursor.fetchall()
            for row in imported_stats:
                user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                imported_count = row.get('imported_count') if isinstance(row, dict) else row[1]
                if user_id in ratings_by_user:
                    # Добавляем импортированные к существующим (только для отображения личной статистики)
                    ratings_by_user[user_id]['count'] += imported_count
                else:
                    # Если у пользователя только импортированные оценки
                    ratings_by_user[user_id] = {'count': imported_count, 'avg': None}
            
            # Формируем сообщение
            text = "📊 <b>Детальная статистика группы</b>\n\n"
            
            # Общая статистика
            text += "📈 <b>Общая статистика:</b>\n"
            text += f"• Всего фильмов: <b>{total_movies}</b>\n"
            text += f"• Просмотрено: <b>{watched_movies}</b>\n"
            text += f"• Всего оценок: <b>{total_ratings}</b>\n"
            text += f"• Запланировано: <b>{total_plans}</b>\n"
            
            # Статистика по сериалам (только для групп)
            if is_group:
                text += f"• Сериалов просмотрено: <b>{watched_series_count}</b>\n"
                text += f"• Сериалы в процессе и в ожидании: <b>{in_progress_series_count}</b>\n"
            
            text += "\n"
            
            # Статистика по участникам
            if users_stats:
                text += "👥 <b>Участники группы:</b>\n"
                for idx, user_row in enumerate(users_stats[:10], 1):  # Показываем топ-10
                    # users_stats теперь список словарей
                    user_id = user_row.get('user_id')
                    username = user_row.get('username')
                    command_count = user_row.get('command_count', 0)
                    
                    user_display = username or f"user_{user_id}"
                    rating_info = ratings_by_user.get(user_id, {})
                    if rating_info:
                        text += f"{idx}. <b>{user_display}</b>\n"
                        text += f"   • Команд: {command_count}\n"
                        text += f"   • Оценок: {rating_info.get('count', 0)}\n"
                        if rating_info.get('avg'):
                            text += f"   • Средняя оценка: {rating_info['avg']:.1f}/10\n"
                    else:
                        text += f"{idx}. <b>{user_display}</b>\n"
                        text += f"   • Команд: {command_count}\n"
                    text += "\n"
                
                if len(users_stats) > 10:
                    text += f"<i>... и ещё {len(users_stats) - 10} участников</i>\n"
            else:
                text += "👥 <i>Нет данных об участниках</i>\n"
            
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
                
                # Жанры (исключаем импортированные фильмы)
                cursor.execute('''
                    SELECT m.genres FROM movies m
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
                genre_counts = {}
                for row in cursor.fetchall():
                    genres = row.get('genres') if isinstance(row, dict) else row[0]
                    if genres:
                        for g in str(genres).split(', '):
                            if g.strip():
                                genre_counts[g.strip()] = genre_counts.get(g.strip(), 0) + 1
                fav_genre = max(genre_counts, key=genre_counts.get) if genre_counts else "—"
                
                # Режиссёры - используем оценки из таблицы ratings (исключаем импортированные)
                cursor.execute('''
                    SELECT m.director, AVG(r.rating) as avg_rating, COUNT(DISTINCT m.id) as film_count
                    FROM movies m
                    LEFT JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                        AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                    WHERE m.chat_id = %s AND m.watched = 1 AND m.director IS NOT NULL AND m.director != %s
                    AND NOT (
                        NOT EXISTS (
                            SELECT 1 FROM ratings r2 
                            WHERE r2.chat_id = m.chat_id 
                            AND r2.film_id = m.id 
                            AND (r2.is_imported = FALSE OR r2.is_imported IS NULL)
                        )
                        AND EXISTS (
                            SELECT 1 FROM ratings r3 
                            WHERE r3.chat_id = m.chat_id 
                            AND r3.film_id = m.id 
                            AND r3.is_imported = TRUE
                        )
                    )
                    GROUP BY m.director
                ''', (chat_id, 'Не указан'))
                director_stats = {}
                for row in cursor.fetchall():
                    d = row.get('director') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                    avg_r = row.get('avg_rating') if isinstance(row, dict) else (row[1] if len(row) > 1 else None)
                    film_count = row.get('film_count') if isinstance(row, dict) else (row[2] if len(row) > 2 else 0)
                    if d and avg_r:  # Только если есть неимпортированные оценки
                        director_stats[d] = {
                            'count': film_count,
                            'sum_rating': (avg_r * film_count) if avg_r else 0,
                            'avg_rating': avg_r if avg_r else 0
                        }
                top_directors = sorted(director_stats.items(), key=lambda x: (-x[1]['count'], -x[1]['avg_rating']))[:3]
                
                # Актёры - используем оценки из таблицы ratings (исключаем импортированные)
                cursor.execute('''
                    SELECT m.actors, AVG(r.rating) as avg_rating, COUNT(DISTINCT m.id) as film_count
                    FROM movies m
                    LEFT JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                        AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                    WHERE m.chat_id = %s AND m.watched = 1
                    AND NOT (
                        NOT EXISTS (
                            SELECT 1 FROM ratings r2 
                            WHERE r2.chat_id = m.chat_id 
                            AND r2.film_id = m.id 
                            AND (r2.is_imported = FALSE OR r2.is_imported IS NULL)
                        )
                        AND EXISTS (
                            SELECT 1 FROM ratings r3 
                            WHERE r3.chat_id = m.chat_id 
                            AND r3.film_id = m.id 
                            AND r3.is_imported = TRUE
                        )
                    )
                    GROUP BY m.actors
                ''', (chat_id,))
                actor_stats = {}
                for row in cursor.fetchall():
                    actors_str = row.get('actors') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                    avg_r = row.get('avg_rating') if isinstance(row, dict) else (row[1] if len(row) > 1 else None)
                    film_count = row.get('film_count') if isinstance(row, dict) else (row[2] if len(row) > 2 else 0)
                    if actors_str and avg_r:  # Только если есть неимпортированные оценки
                        for a in actors_str.split(', '):
                            a = a.strip()
                            if a and a != "—":
                                if a not in actor_stats:
                                    actor_stats[a] = {'count': 0, 'sum_rating': 0, 'total_ratings': 0}
                                # Для актеров считаем количество фильмов, где они участвовали
                                actor_stats[a]['count'] += film_count
                                # Суммируем средние оценки, умноженные на количество фильмов
                                if avg_r:
                                    actor_stats[a]['sum_rating'] += avg_r * film_count
                                    actor_stats[a]['total_ratings'] += film_count
                
                # Пересчитываем средние для актеров
                for actor in actor_stats:
                    if actor_stats[actor]['total_ratings'] > 0:
                        actor_stats[actor]['avg_rating'] = actor_stats[actor]['sum_rating'] / actor_stats[actor]['total_ratings']
                    else:
                        actor_stats[actor]['avg_rating'] = 0
                
                top_actors = sorted(actor_stats.items(), key=lambda x: (-x[1]['count'], -x[1].get('avg_rating', 0)))[:3]
                
                # Рассчитываем среднее из ratings (исключаем импортированные)
                cursor.execute('SELECT AVG(rating) FROM ratings WHERE chat_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id,))
                avg_row = cursor.fetchone()
                avg_rating = avg_row.get('avg') if isinstance(avg_row, dict) else (avg_row[0] if avg_row and len(avg_row) > 0 else None)
                avg_str = f"{avg_rating:.1f}/10" if avg_rating else "—"
                
                text = f"📊 <b>Статистика кино-группы</b>\n\n"
                text += f"🎬 Всего фильмов: <b>{total}</b>\n"
                text += f"✅ Просмотрено: <b>{watched}</b>\n"
                text += f"⏳ Ждёт просмотра: <b>{unwatched}</b>\n"
                text += f"🌟 Средняя оценка: <b>{avg_str}</b>\n"
                text += f"❤️ Любимый жанр: <b>{fav_genre}</b>\n\n"
                
                if top_directors:
                    text += "<b>Топ режиссёров:</b>\n"
                    for d, stats in top_directors:
                        avg_d = stats.get('avg_rating', 0) if stats.get('avg_rating') else 0
                        text += f"• {d} — {stats['count']} фильм(ов), средняя {avg_d:.1f}/10\n"
                    text += "\n"
                else:
                    text += "<b>Топ режиссёров:</b> —\n\n"
                
                if top_actors:
                    text += "<b>Топ актёров:</b>\n"
                    for a, stats in top_actors:
                        avg_a = stats.get('avg_rating', 0) if stats.get('avg_rating') else 0
                        text += f"• {a} — {stats['count']} фильм(ов), средняя {avg_a:.1f}/10\n"
                else:
                    text += "<b>Топ актёров:</b> —\n"
                
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
            
            # Статистика промокодов
            from moviebot.utils.promo import get_promocode_statistics
            promo_stats = get_promocode_statistics()
            text += "🏷️ <b>Промокоды:</b>\n"
            text += f"   • Всего промокодов: {promo_stats.get('total_promocodes', 0)}\n"
            text += f"   • Активных: {promo_stats.get('active_promocodes', 0)}\n"
            text += f"   • Использовано: {promo_stats.get('total_uses', 0)}\n"
            if promo_stats.get('promocodes'):
                text += "   • Детали:\n"
                for promo in promo_stats['promocodes'][:5]:  # Показываем первые 5
                    discount_str = f"{promo['discount_value']}%" if promo['discount_type'] == 'percent' else f"{int(promo['discount_value'])} руб/звезд"
                    status = "✅" if promo['is_active'] else "❌"
                    text += f"     {status} {promo['code']} ({discount_str}) — использовано: {promo['used_count']}/{promo['total_uses']}, осталось: {promo['remaining']}\n"
            text += "\n"
            
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
            
            # Если charge_id указан в команде, обрабатываем сразу
            if len(parts) >= 2:
                charge_id = parts[1].strip()
                _process_refund(message, charge_id)
                return
            
            # Если charge_id не указан, запрашиваем его
            from moviebot.states import user_refund_state
            user_id = message.from_user.id
            user_refund_state[user_id] = {'chat_id': message.chat.id}
            bot_instance.reply_to(message, "📝 Укажите ID операции (charge_id) для возврата.\n\n"
                                  "Отправьте ID операции в ответ на это сообщение.\n\n"
                                  "Пример: stxwe_iXQAPRqkiZSjm9JxEiO0Ke03gNqoupstFOak10sj3ZSSeHbT2_3MukFRW4kGE-YBSssodFt05T9Szh1-N2m_FgDCvAAPloyRiqVDUp3tmzfl2I891zLP4VcZ6ul8I")
            logger.info(f"[REFUND] Ожидаем ввод charge_id от пользователя {user_id}")
            
        except Exception as e:
            logger.error(f"Ошибка в refundstars_command: {e}", exc_info=True)
            try:
                bot_instance.reply_to(message, f"❌ Ошибка обработки команды: {e}")
            except:
                pass
