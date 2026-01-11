"""
Модуль для задач планировщика
"""
# 1. Стандартная библиотека Python
import logging
import random
import time
import pytz

from datetime import datetime, timedelta, date

# 2. Сторонние библиотеки (в алфавитном порядке)
import telebot
from telebot.apihelper import ApiTelegramException
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# 3. APScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# 4. Твои локальные импорты (отсортируй по алфавиту внутри группы)
from moviebot.bot.bot_init import bot, BOT_ID
from moviebot.database.db_connection import db_lock
from moviebot.config import PLANS_TZ
from moviebot.api.kinopoisk_api import get_seasons_data
from moviebot.api.kinopoisk_api import get_external_sources

# Импортируем ТОЛЬКО функцию, а не глобальные conn/cursor/db_lock
from moviebot.database.db_connection import get_db_connection

# Импорт helpers отключён полностью — все нужные функции определены в этом же файле (scheduler.py)
# from moviebot.utils.helpers import (...)
from moviebot.database.db_operations import get_user_timezone_or_default, get_notification_settings
from moviebot.bot.handlers.seasons import get_series_airing_status

logger = logging.getLogger(__name__)

conn = get_db_connection()
cursor = conn.cursor()
plans_tz = PLANS_TZ  # Для обратной совместимости

# bot и scheduler будут установлены из main.py
bot = None
scheduler = None

def set_bot_instance(new_bot):
    """Устанавливает экземпляр бота для использования в задачах"""
    global bot
    bot = new_bot  # ← Исправлено: было bot_instance — теперь правильно присваиваем переданный bot

def set_scheduler_instance(new_scheduler):
    """Устанавливает экземпляр scheduler для использования в задачах"""
    global scheduler
    scheduler = new_scheduler

def hourly_stats():
    """Вызывается каждый час для вывода статистики"""
    print_daily_stats()



# Функции для уведомлений о планах (определяем до использования в scheduler)
def send_plan_notification(chat_id, film_id, title, link, plan_type, plan_id=None, user_id=None):
    """Отправляет уведомление о запланированном просмотре"""
    try:
        plan_type_text = "дома" if plan_type == 'home' else "в кино"
        text = f"🔔 Напоминание: сегодня запланирован просмотр {plan_type_text}!\n\n"
        text += f"<b>{title}</b>\n{link}"
       
        markup = None
       
        # Проверяем, является ли фильм сериалом, и получаем информацию о последней просмотренной серии
        is_series = False
        last_episode_info = None
        if user_id and film_id:
            with db_lock:
                cursor.execute('SELECT is_series FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                movie_row = cursor.fetchone()
                if movie_row:
                    is_series = bool(movie_row.get('is_series') if isinstance(movie_row, dict) else movie_row[0])
                   
                    if is_series:
                        cursor.execute('''
                            SELECT season_number, episode_number
                            FROM series_tracking
                            WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                            ORDER BY season_number DESC, episode_number DESC
                            LIMIT 1
                        ''', (chat_id, film_id, user_id))
                        last_episode_row = cursor.fetchone()
                        if last_episode_row:
                            if isinstance(last_episode_row, dict):
                                last_episode_info = {
                                    'season': last_episode_row.get('season_number'),
                                    'episode': last_episode_row.get('episode_number')
                                }
                            else:
                                last_episode_info = {
                                    'season': last_episode_row[0],
                                    'episode': last_episode_row[1]
                                }
       
        if is_series and last_episode_info:
            text += f"\n\n📺 <b>Последняя просмотренная серия:</b> Сезон {last_episode_info['season']}, Серия {last_episode_info['episode']}"
       
        has_access = False
        if user_id:
            has_access = has_notifications_access(chat_id, user_id)
       
        if not has_access and user_id:
            text += "\n\n💡 <b>Вы можете отслеживать просмотренные серии и подключить напоминания о выходе новых серий с тарифом 🔔 Уведомления</b>"
       
        # Для планов "дома" — существующий код с онлайн-кинотеатрами
        if plan_type == 'home' and plan_id:
            with db_lock:
                cursor.execute('''
                    SELECT streaming_service, streaming_url, streaming_done, ticket_file_id
                    FROM plans
                    WHERE id = %s AND chat_id = %s
                ''', (plan_id, chat_id))
                plan_row = cursor.fetchone()
               
                if plan_row:
                    if isinstance(plan_row, dict):
                        streaming_service = plan_row.get('streaming_service')
                        streaming_url = plan_row.get('streaming_url')
                        streaming_done = plan_row.get('streaming_done', False)
                        ticket_file_id = plan_row.get('ticket_file_id')
                    else:
                        streaming_service = plan_row[0] if plan_row else None
                        streaming_url = plan_row[1] if len(plan_row) > 1 else None
                        streaming_done = plan_row[2] if len(plan_row) > 2 else False
                        ticket_file_id = plan_row[3] if len(plan_row) > 3 else None
                   
                    if streaming_done:
                        logger.info(f"[PLAN NOTIFICATION] streaming_done=True для плана {plan_id}, кинотеатры не показываем")
                    elif streaming_service and streaming_url:
                        text += f"\n\n📺 <b>Онлайн-кинотеатр:</b> <a href='{streaming_url}'>{streaming_service}</a>"
                        logger.info(f"[PLAN NOTIFICATION] Показываем ссылку на кинотеатр {streaming_service} для плана {plan_id}")
                    else:
                        # ... (твой код с кнопками кинотеатров остаётся без изменений)
                        cursor.execute('SELECT kp_id FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                        movie_row = cursor.fetchone()
                        kp_id = None
                        if movie_row:
                            kp_id = movie_row.get('kp_id') if isinstance(movie_row, dict) else movie_row[0]
                       
                        sources_dict = {}
                        if ticket_file_id:
                            try:
                                sources_dict = json.loads(ticket_file_id)
                            except:
                                pass
                       
                        if not sources_dict and kp_id:
                            if sources:
                                sources_dict = {platform: url for platform, url in sources[:6]}
                                sources_json = json.dumps(sources_dict, ensure_ascii=False)
                                cursor.execute('''
                                    UPDATE plans
                                    SET ticket_file_id = %s
                                    WHERE id = %s AND chat_id = %s
                                ''', (sources_json, plan_id, chat_id))
                                conn.commit()
                       
                        if sources_dict:
                            if not markup:
                                markup = InlineKeyboardMarkup(row_width=2)
                            for platform, url in list(sources_dict.items())[:6]:
                                markup.add(InlineKeyboardButton(platform, url=url))
                            text += f"\n\n📺 <b>Выберите онлайн-кинотеатр для просмотра:</b>"
                            logger.info(f"[PLAN NOTIFICATION] Показываем кнопки с кинотеатрами для плана {plan_id}")
       
        # Новый блок для планов "в кино"
        elif plan_type == 'cinema' and plan_id:
            with db_lock:
                cursor.execute('SELECT ticket_file_id FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
                row = cursor.fetchone()
                ticket_file_id = None
                if row:
                    if isinstance(row, dict):
                        ticket_file_id = row.get('ticket_file_id')
                    else:
                        ticket_file_id = row[0]
               
                if not markup:
                    markup = InlineKeyboardMarkup()
               
                if not ticket_file_id or str(ticket_file_id).strip() == '' or ticket_file_id == 'null':
                    markup.add(InlineKeyboardButton("📸 Добавить билеты", callback_data=f"add_ticket:{plan_id}"))
                    text += "\n\n🎟 Не забудьте добавить фото билетов!"
                    logger.info(f"[PLAN NOTIFICATION] Кнопка 'Добавить билеты' для плана {plan_id}")
                else:
                    markup.add(InlineKeyboardButton("🎟 Показать билеты", callback_data=f"show_ticket:{plan_id}"))
                    logger.info(f"[PLAN NOTIFICATION] Кнопка 'Показать билеты' для плана {plan_id}")

        # Кнопка подписки в конце
        if not has_access and user_id:
            if not markup:
                markup = InlineKeyboardMarkup()
            subscription_type = 'personal' if chat_id > 0 else 'group'
            markup.add(InlineKeyboardButton("🔔 Перейти к подписке", callback_data=f"payment:tariffs:{subscription_type}"))
       
        msg = bot.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup)
       
        plan_notification_messages[msg.message_id] = {
            'link': link,
            'film_id': film_id,
            'plan_id': plan_id
        }
       
        logger.info(f"[PLAN NOTIFICATION] Уведомление отправлено для фильма {title} в чат {chat_id}, message_id={msg.message_id}, plan_id={plan_id}")
       
        if plan_id:
            try:
                with db_lock:
                    cursor.execute('UPDATE plans SET notification_sent = TRUE WHERE id = %s', (plan_id,))
                    conn.commit()
                logger.info(f"[PLAN NOTIFICATION] План {plan_id} отмечен как уведомление отправлено")
            except Exception as e:
                logger.warning(f"[PLAN NOTIFICATION] Не удалось отметить план {plan_id} как отправленный: {e}")

    except Exception as e:
        logger.error(f"[PLAN NOTIFICATION] Ошибка отправки уведомления: {e}")

def send_ticket_notification(chat_id, plan_id):
    """Отправляет напоминание с билетами за 10 минут до сеанса"""
    try:
        with db_lock:
            cursor.execute('''
                SELECT p.ticket_file_id, m.title, p.plan_datetime
                FROM plans p
                JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.id = %s AND p.chat_id = %s
            ''', (plan_id, chat_id))
            ticket_row = cursor.fetchone()
        
        if not ticket_row:
            logger.warning(f"[TICKET NOTIFICATION] План не найден для plan_id={plan_id}")
            return
        
        if isinstance(ticket_row, dict):
            ticket_file_id = ticket_row.get('ticket_file_id')
            title = ticket_row.get('title')
            plan_dt_value = ticket_row.get('plan_datetime')
        else:
            ticket_file_id = ticket_row.get("ticket_file_id") if isinstance(ticket_row, dict) else (ticket_row[0] if ticket_row else None)
            title = ticket_row[1]
            plan_dt_value = ticket_row[2]
        
        if not ticket_file_id:
            logger.warning(f"[TICKET NOTIFICATION] Билеты не найдены для plan_id={plan_id}")
            return
        
        # Парсим билеты (может быть JSON массив или один file_id)
        ticket_files = []
        try:
            ticket_files = json.loads(ticket_file_id)
            if not isinstance(ticket_files, list):
                ticket_files = [ticket_file_id]
        except:
            # Старый формат - один file_id
            ticket_files = [ticket_file_id]
        
        text = f"🎟️ <b>Напоминание: через 10 минут сеанс!</b>\n\n<b>{title}</b>\n\nВаши билеты ({len(ticket_files)} шт.):"
        
        # Отправляем все билеты
        sent_count = 0
        for i, file_id in enumerate(ticket_files):
            try:
                if i == 0:
                    caption = text
                else:
                    caption = f"🎟️ Билет {i+1}/{len(ticket_files)}"
                
                bot.send_photo(chat_id, file_id, caption=caption, parse_mode='HTML')
                sent_count += 1
            except:
                try:
                    bot.send_document(chat_id, file_id, caption=caption, parse_mode='HTML')
                    sent_count += 1
                except Exception as e:
                    logger.error(f"[TICKET NOTIFICATION] Ошибка отправки билета {i+1}: {e}")
        
        if sent_count == 0:
            # Если не удалось отправить ни одного билета, отправляем текстовое сообщение
            bot.send_message(chat_id, f"🎟️ <b>Напоминание: через 10 минут сеанс!</b>\n\n<b>{title}</b>", parse_mode='HTML')
        
        # Отмечаем как отправленное в базе данных
        try:
            with db_lock:
                cursor.execute('''
                    UPDATE plans 
                    SET ticket_notification_sent = TRUE 
                    WHERE id = %s
                ''', (plan_id,))
                conn.commit()
            logger.info(f"[TICKET NOTIFICATION] План {plan_id} отмечен как уведомление с билетами отправлено")
        except Exception as e:
            logger.warning(f"[TICKET NOTIFICATION] Не удалось отметить план {plan_id} как отправленный: {e}")
        
        logger.info(f"[TICKET NOTIFICATION] Напоминание с билетами отправлено для {title} в чат {chat_id}")
    except Exception as e:
        logger.error(f"[TICKET NOTIFICATION] Ошибка отправки напоминания: {e}", exc_info=True)


def check_and_send_plan_notifications():
    """Периодическая проверка планов и отправка пропущенных уведомлений"""

    try:

        now_utc = datetime.now(pytz.utc)

        # Проверяем планы на ближайшие сутки и пропущенные за последние 30 минут

        check_start = now_utc - timedelta(minutes=30)

        check_end = now_utc + timedelta(days=1)

        

        # КРИТИЧЕСКИЙ ФИКС: Добавляем rollback при ошибках транзакции
        try:
            # Сначала делаем rollback на случай если предыдущая транзакция упала
            from moviebot.database.db_connection import conn
            try:
                conn.rollback()
            except:
                pass
        except:
            pass
        
        with db_lock:
            try:
                cursor.execute('''

                    SELECT p.id, p.chat_id, p.film_id, p.plan_type, p.plan_datetime, p.user_id,

                           m.title, m.link, p.notification_sent, p.ticket_notification_sent

                    FROM plans p

                    JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id

                    WHERE p.plan_datetime >= %s 

                      AND p.plan_datetime <= %s

                ''', (check_start, check_end))

                plans = cursor.fetchall()
            except Exception as db_e:
                logger.error(f"[PLAN CHECK] Ошибка при запросе планов: {db_e}", exc_info=True)
                try:
                    conn.rollback()
                except:
                    pass
                plans = []

        

        if not plans:

            return

        

        logger.info(f"[PLAN CHECK] Проверяем {len(plans)} планов на уведомления")

        

        for plan in plans:

            if isinstance(plan, dict):

                plan_id = plan.get('id')

                chat_id = plan.get('chat_id')

                film_id = plan.get('film_id')

                plan_type = plan.get('plan_type')

                plan_datetime = plan.get('plan_datetime')

                user_id = plan.get('user_id')

                title = plan.get('title')

                link = plan.get('link')

                notification_sent = plan.get('notification_sent', False)

                ticket_notification_sent = plan.get('ticket_notification_sent', False)

            else:

                plan_id = plan[0]

                chat_id = plan[1]

                film_id = plan[2]

                plan_type = plan[3]

                plan_datetime = plan[4]

                user_id = plan[5]

                title = plan[6]

                link = plan[7]

                notification_sent = plan[8] if len(plan) > 8 else False

                ticket_notification_sent = plan[9] if len(plan) > 9 else False

            

            # Получаем часовой пояс пользователя

            user_tz = get_user_timezone_or_default(user_id)

            

            # Преобразуем plan_datetime в локальное время пользователя

            if isinstance(plan_datetime, datetime):

                if plan_datetime.tzinfo is None:

                    plan_dt_local = pytz.utc.localize(plan_datetime).astimezone(user_tz)

                else:

                    plan_dt_local = plan_datetime.astimezone(user_tz)

            else:

                plan_dt_local = datetime.fromisoformat(str(plan_datetime).replace('Z', '+00:00')).astimezone(user_tz)

            

            now_local = datetime.now(user_tz)

            

            if plan_type == 'cinema':

                # Для планов в кино проверяем два типа уведомлений:

                # 1. Напоминание в день сеанса (только если это сегодня)
                # Время зависит от дня недели и настроек

                if plan_dt_local.date() == now_local.date():
                    # Получаем настройки времени напоминаний
                    notify_settings = get_notification_settings(chat_id)
                    
                    # Определяем, будний день или выходной
                    weekday = plan_dt_local.weekday()
                    is_weekend = weekday >= 5
                    
                    # Если разделение на будни/выходные отключено, используем настройки для будних дней
                    if notify_settings.get('separate_weekdays') == 'false':
                        reminder_hour = notify_settings.get('cinema_weekday_hour', 9)
                        reminder_minute = notify_settings.get('cinema_weekday_minute', 0)
                    elif is_weekend:
                        reminder_hour = notify_settings.get('cinema_weekend_hour', 9)
                        reminder_minute = notify_settings.get('cinema_weekend_minute', 0)
                    else:
                        reminder_hour = notify_settings.get('cinema_weekday_hour', 9)
                        reminder_minute = notify_settings.get('cinema_weekday_minute', 0)

                    reminder_dt = plan_dt_local.replace(hour=reminder_hour, minute=reminder_minute)

                    reminder_utc = reminder_dt.astimezone(pytz.utc)
                else:

                    reminder_utc = None

                

                # Планируем напоминание, если оно еще не запланировано и время еще не прошло
                # Проверяем, не было ли уже отправлено уведомление
                if reminder_utc and reminder_utc > now_utc and not notification_sent:

                    try:

                        job_id = f'plan_reminder_{chat_id}_{plan_id}_{int(reminder_utc.timestamp())}'

                        existing_job = scheduler.get_job(job_id)

                        if not existing_job:

                            scheduler.add_job(

                                send_plan_notification,

                                'date',

                                run_date=reminder_utc,

                                args=[chat_id, film_id, title, link, plan_type, plan_id],

                                id=job_id

                            )

                            logger.info(f"[PLAN CHECK] Запланировано напоминание для плана кино {plan_id} (фильм {title}) на {reminder_utc} ({reminder_hour}:{reminder_minute:02d})")

                    except Exception as e:

                        logger.warning(f"[PLAN CHECK] Не удалось запланировать напоминание для плана {plan_id}: {e}")

                elif reminder_utc and reminder_utc <= now_utc and reminder_utc >= now_utc - timedelta(minutes=30):

                    # Время напоминания уже прошло, но не более 30 минут назад - отправляем сразу
                    # Проверяем, не было ли уже отправлено уведомление
                    if not notification_sent:
                        try:
                            # Проверяем, не запланировано ли уже уведомление
                            job_id = f'plan_reminder_{chat_id}_{plan_id}_{int(reminder_utc.timestamp())}'
                            existing_job = scheduler.get_job(job_id)
                            if not existing_job:
                                send_plan_notification(chat_id, film_id, title, link, plan_type, plan_id=plan_id, user_id=user_id)
                                logger.info(f"[PLAN CHECK] Напоминание отправлено сразу для плана кино {plan_id} (фильм {title})")
                            else:
                                logger.info(f"[PLAN CHECK] Напоминание уже запланировано для плана кино {plan_id}")
                        except Exception as e:
                            logger.error(f"[PLAN CHECK] Ошибка отправки напоминания для плана {plan_id}: {e}", exc_info=True)
                    else:
                        logger.info(f"[PLAN CHECK] Напоминание уже отправлено для плана кино {plan_id}, пропускаем")

                

                # 2. Напоминание с билетами за N минут до сеанса (из настроек)
                notify_settings = get_notification_settings(chat_id)
                ticket_before_minutes = notify_settings.get('ticket_before_minutes', 10)
                
                # Если настройка "не присылать отдельно" или "вместе с уведомлением", пропускаем
                if ticket_before_minutes == -1:  # -1 означает "не присылать отдельно"
                    ticket_utc = None
                elif ticket_before_minutes == 0:  # 0 означает "вместе с уведомлением"
                    # Билеты будут отправлены вместе с основным уведомлением
                    ticket_utc = None
                else:
                    ticket_dt = plan_dt_local - timedelta(minutes=ticket_before_minutes)
                    ticket_utc = ticket_dt.astimezone(pytz.utc)

                

                # Проверяем наличие билетов (используем ticket_file_id из plans)

                with db_lock:

                    cursor.execute('SELECT ticket_file_id FROM plans WHERE id = %s', (plan_id,))

                    ticket_row = cursor.fetchone()

                    ticket_file_id = ticket_row.get('ticket_file_id') if isinstance(ticket_row, dict) else (ticket_row[0] if ticket_row else None)

                

                if ticket_file_id and ticket_utc:

                    # Планируем напоминание с билетами, если оно еще не запланировано и время еще не прошло
                    # Проверяем, не было ли уже отправлено уведомление с билетами
                    if ticket_utc > now_utc and not ticket_notification_sent:

                        try:

                            job_id = f'ticket_notify_{chat_id}_{plan_id}_{int(ticket_utc.timestamp())}'

                            existing_job = scheduler.get_job(job_id)

                            if not existing_job:

                                scheduler.add_job(

                                    send_ticket_notification,

                                    'date',

                                    run_date=ticket_utc,

                                    args=[chat_id, plan_id],

                                    id=job_id

                                )

                                logger.info(f"[PLAN CHECK] Запланировано уведомление с билетами для плана {plan_id} (фильм {title}) на {ticket_utc}")

                        except Exception as e:

                            logger.warning(f"[PLAN CHECK] Не удалось запланировать уведомление с билетами для плана {plan_id}: {e}")

                    elif ticket_utc <= now_utc and ticket_utc >= now_utc - timedelta(minutes=30):

                        # Время напоминания с билетами уже прошло, но не более 30 минут назад - отправляем сразу
                        # Проверяем, не было ли уже отправлено уведомление с билетами
                        if not ticket_notification_sent:
                            try:
                                # Проверяем, не запланировано ли уже уведомление
                                job_id = f'ticket_notify_{chat_id}_{plan_id}_{int(ticket_utc.timestamp())}'
                                existing_job = scheduler.get_job(job_id)
                                if not existing_job:
                                    send_ticket_notification(chat_id, plan_id)
                                    # Отмечаем как отправленное
                                    with db_lock:
                                        cursor.execute('UPDATE plans SET ticket_notification_sent = TRUE WHERE id = %s', (plan_id,))
                                        conn.commit()
                                    logger.info(f"[PLAN CHECK] Уведомление с билетами отправлено сразу для плана {plan_id} (фильм {title})")
                                else:
                                    logger.info(f"[PLAN CHECK] Уведомление с билетами уже запланировано для плана {plan_id}")
                            except Exception as e:
                                logger.error(f"[PLAN CHECK] Ошибка отправки уведомления с билетами для плана {plan_id}: {e}", exc_info=True)
                        else:
                            logger.info(f"[PLAN CHECK] Уведомление с билетами уже отправлено для плана {plan_id}, пропускаем")

            else:

                # Для планов дома проверяем уведомления:
                # 1. Напоминание на время плана (если план в будущем)
                # 2. Напоминание в стандартное время (если план на сегодня и время совпадает со стандартным)

                # Получаем настройки времени напоминаний
                notify_settings = get_notification_settings(chat_id)
                
                # Определяем, будний день или выходной (0 = понедельник, 6 = воскресенье)
                weekday = plan_dt_local.weekday()  # 0-6, где 0 = понедельник, 6 = воскресенье
                is_weekend = weekday >= 5  # Суббота (5) и воскресенье (6)
                
                # Определяем стандартное время напоминания
                if notify_settings.get('separate_weekdays') == 'false':
                    default_hour = notify_settings.get('home_weekday_hour', 19)
                    default_minute = notify_settings.get('home_weekday_minute', 0)
                elif is_weekend:
                    default_hour = notify_settings.get('home_weekend_hour', 9)
                    default_minute = notify_settings.get('home_weekend_minute', 0)
                else:
                    default_hour = notify_settings.get('home_weekday_hour', 19)
                    default_minute = notify_settings.get('home_weekday_minute', 0)

                # Проверяем, совпадает ли время плана со стандартным временем
                plan_hour = plan_dt_local.hour
                plan_minute = plan_dt_local.minute
                is_default_time = (plan_hour == default_hour and plan_minute == default_minute)

                # 1. Напоминание на время плана (для всех планов, если время еще не прошло)
                plan_utc = plan_dt_local.astimezone(pytz.utc)
                
                if plan_utc > now_utc and not notification_sent:
                    # План в будущем - планируем уведомление на время плана
                    try:
                        job_id = f'plan_notify_home_{chat_id}_{plan_id}_{int(plan_utc.timestamp())}'
                        existing_job = scheduler.get_job(job_id)
                        
                        if not existing_job:
                            scheduler.add_job(
                                send_plan_notification,
                                'date',
                                run_date=plan_utc,
                                args=[chat_id, film_id, title, link, plan_type, plan_id, user_id],
                                id=job_id
                            )
                            logger.info(f"[PLAN CHECK] Запланировано уведомление для плана дома {plan_id} (фильм {title}) на время плана {plan_utc} ({plan_hour:02d}:{plan_minute:02d})")
                    except Exception as e:
                        logger.warning(f"[PLAN CHECK] Не удалось запланировать уведомление на время плана {plan_id}: {e}")
                        
                elif plan_utc <= now_utc and plan_utc >= now_utc - timedelta(minutes=30):
                    # Время плана уже прошло, но не более 30 минут назад - отправляем сразу
                    if not notification_sent:
                        try:
                            job_id = f'plan_notify_home_{chat_id}_{plan_id}_{int(plan_utc.timestamp())}'
                            existing_job = scheduler.get_job(job_id)
                            if not existing_job:
                                send_plan_notification(chat_id, film_id, title, link, plan_type, plan_id=plan_id, user_id=user_id)
                                logger.info(f"[PLAN CHECK] Уведомление отправлено сразу для плана дома {plan_id} (фильм {title}) на время плана {plan_utc}")
                            else:
                                logger.info(f"[PLAN CHECK] Уведомление уже запланировано для плана дома {plan_id}")
                        except Exception as e:
                            logger.error(f"[PLAN CHECK] Ошибка отправки уведомления для плана {plan_id}: {e}", exc_info=True)
                    else:
                        logger.info(f"[PLAN CHECK] Уведомление уже отправлено для плана дома {plan_id}, пропускаем")

    except Exception as e:

        logger.error(f"[PLAN CHECK] Ошибка при проверке планов: {e}", exc_info=True)



# Настройка периодического вывода статистики
# Вызовы scheduler.add_job должны быть в moviebot.py после импорта модуля



# Очистка планов

def clean_home_plans():
    """Ежедневно удаляет планы дома на вчерашний день, если по фильму нет оценок.
    Также удаляет все планы дома на прошедшие выходные (суббота и воскресенье) в понедельник."""

    now = datetime.now(plans_tz)
    today = now.date()
    yesterday = (now - timedelta(days=1)).date()
    today_weekday = today.weekday()  # 0 = Monday, 6 = Sunday

    deleted_count = 0

    with db_lock:
        # Если сегодня понедельник, удаляем все планы дома на прошедшие выходные (суббота и воскресенье)
        if today_weekday == 0:  # Monday
            # Находим субботу и воскресенье прошлой недели
            saturday = yesterday - timedelta(days=1)  # Вчера было воскресенье, значит суббота - позавчера
            sunday = yesterday

            cursor.execute('''
                SELECT p.id, p.film_id, p.chat_id, m.title
                FROM plans p
                JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.plan_type = 'home' 
                AND DATE(p.plan_datetime AT TIME ZONE 'Europe/Moscow') IN (%s, %s)
            ''', (saturday, sunday))

            weekend_rows = cursor.fetchall()

            for row in weekend_rows:
                plan_id = row.get('id') if isinstance(row, dict) else row[0]
                film_id = row.get('film_id') if isinstance(row, dict) else row[1]
                chat_id = row.get('chat_id') if isinstance(row, dict) else row[2]
                title = row.get('title') if isinstance(row, dict) else row[3]
                
                cursor.execute('DELETE FROM plans WHERE id = %s', (plan_id,))
                deleted_count += 1
                
                if bot:
                    try:
                        bot.send_message(chat_id, f"📅 План на фильм <b>{title}</b> удалён (выходные прошли).", parse_mode='HTML')
                    except:
                        pass
            
            logger.info(f"Очищены планы дома на выходные: {len(weekend_rows)} планов")
        
        # Находим планы дома на вчера (используем AT TIME ZONE для корректной работы с TIMESTAMP WITH TIME ZONE)
        cursor.execute('''
            SELECT p.id, p.film_id, p.chat_id
            FROM plans p
            WHERE p.plan_type = 'home' AND DATE(p.plan_datetime AT TIME ZONE 'Europe/Moscow') = %s
        ''', (yesterday,))

        rows = cursor.fetchall()

        for row in rows:
            # RealDictCursor возвращает словари, но поддерживает доступ по индексу
            plan_id = row.get('id') if isinstance(row, dict) else row[0]
            film_id = row.get('film_id') if isinstance(row, dict) else row[1]
            chat_id = row.get('chat_id') if isinstance(row, dict) else row[2]

            # Проверяем, есть ли оценки по фильму
            cursor.execute('SELECT COUNT(*) FROM ratings WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))

            count_row = cursor.fetchone()

            count = count_row.get('count') if isinstance(count_row, dict) else (count_row[0] if count_row else 0)

            if count == 0:
                cursor.execute('DELETE FROM plans WHERE id = %s', (plan_id,))
                deleted_count += 1

                if bot:
                    try:
                        bot.send_message(chat_id, f"📅 План на фильм удалён (нет оценок за вчера).")
                    except:
                        pass

        conn.commit()

    logger.info(f"Очищены планы дома без оценок: {deleted_count} планов")



def clean_cinema_plans():
    """Каждый понедельник удаляет все планы кино"""

    with db_lock:

        cursor.execute("DELETE FROM plans WHERE plan_type = 'cinema'")

        deleted_count = cursor.rowcount

        conn.commit()

    

    logger.info(f"Очищены все планы кино (понедельник): {deleted_count} планов")



# Голосование для фильмов "в кино"

def start_cinema_votes():
    """Каждый понедельник в 9:00 запускает голосование для фильмов в кино"""

    now = datetime.now(plans_tz)

    if now.weekday() != 0:  # только понедельник

        return

    

    with db_lock:

        cursor.execute('''

            SELECT p.id, p.film_id, p.chat_id, m.title, m.link

            FROM plans p

            JOIN movies m ON p.film_id = m.id AND m.chat_id = p.chat_id

            WHERE p.plan_type = 'cinema' AND p.plan_datetime < NOW()

        ''')

        rows = cursor.fetchall()

        

        for row in rows:

            # RealDictCursor возвращает словари, но поддерживает доступ по индексу

            plan_id = row.get('id') if isinstance(row, dict) else row[0]

            film_id = row.get('film_id') if isinstance(row, dict) else row[1]

            chat_id = row.get('chat_id') if isinstance(row, dict) else row[2]

            title = row.get('title') if isinstance(row, dict) else row[3]

            link = row.get('link') if isinstance(row, dict) else row[4]

            # Проверяем, есть ли оценки

            cursor.execute('SELECT COUNT(*) FROM ratings WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))

            count_row = cursor.fetchone()

            count = count_row.get('count') if isinstance(count_row, dict) else (count_row[0] if count_row else 0)

            if count > 0:

                continue  # оценки есть — не запускаем голосование

            

            # Запускаем голосование

            deadline = (now.replace(hour=23, minute=59, second=59) + timedelta(days=1)).isoformat()  # конец понедельника

            

            try:

                text = f"📊 Голосование: Оставить в расписании фильм <b>{title}</b> ещё на неделю%s\n{link}\n\nОтветьте \"да\" или \"нет\" (в ответ на это сообщение)."

                msg = bot.send_message(chat_id, text, parse_mode='HTML')

                

                cursor.execute('''

                    INSERT INTO cinema_votes (chat_id, film_id, message_id, deadline)

                    VALUES (%s, %s, %s, %s)

                ''', (chat_id, film_id, msg.message_id, deadline))

                conn.commit()

            except Exception as e:

                logger.error(f"Ошибка при отправке сообщения голосования для фильма {film_id}: {e}")

    

    logger.info(f"Запущены голосования для фильмов в кино")



def resolve_cinema_votes():
    """Во вторник в 9:00 подводит итоги голосований"""

    with db_lock:

        cursor.execute('''

            SELECT chat_id, film_id, yes_users, no_users, m.title

            FROM cinema_votes v

            JOIN movies m ON v.film_id = m.id AND m.chat_id = v.chat_id

            WHERE deadline < NOW()

        ''')

        rows = cursor.fetchall()

        

        for row in rows:

            # RealDictCursor возвращает словари, но поддерживает доступ по индексу

            chat_id = row.get('chat_id') if isinstance(row, dict) else row[0]

            film_id = row.get('film_id') if isinstance(row, dict) else row[1]

            yes_json = row.get('yes_votes') if isinstance(row, dict) else row[2]

            no_json = row.get('no_votes') if isinstance(row, dict) else row[3]

            title = row.get('title') if isinstance(row, dict) else row[4]

            yes_count = len(json.loads(yes_json or '[]'))

            no_count = len(json.loads(no_json or '[]'))

            

            if no_count > yes_count or (yes_count == no_count and no_count > 0):

                cursor.execute('DELETE FROM plans WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))

                try:

                    bot.send_message(chat_id, f"📅 Фильм <b>{title}</b> удалён из расписания по результатам голосования.", parse_mode='HTML')

                except:

                    pass

            else:

                try:

                    bot.send_message(chat_id, f"📅 Фильм <b>{title}</b> остался в расписании на следующую неделю.", parse_mode='HTML')

                except:

                    pass

            

            cursor.execute('DELETE FROM cinema_votes WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))

        conn.commit()

    

    logger.info(f"Подведены итоги для {len(rows)} голосований")



# Добавляем задачи очистки и голосования в scheduler
# Вызовы scheduler.add_job должны быть в moviebot.py после импорта модуля

def send_series_notification(chat_id, film_id, kp_id, title, season, episode):
    """Отправляет уведомление о выходе новой серии и проверяет следующую дату"""
    try:
        if not bot:
            logger.error("[SERIES NOTIFICATION] bot не установлен")
            return
        
        text = f"🔔 <b>Новая серия вышла!</b>\n\n"
        text += f"📺 <b>{title}</b>\n"
        text += f"📅 Сезон {season}, Эпизод {episode}\n\n"
        text += f"<a href='https://www.kinopoisk.ru/series/{kp_id}/'>Кинопоиск</a>\n\n"

        if sources:
            text += "🎬 <b>Смотреть онлайн:</b>\n"
            for platform, url in sources[:4]:  # лимит, чтобы не раздувать сообщение
                text += f"• <a href='{url}'>{platform}</a>\n"
        else:
            # Минимум: только Кинопоиск HD (самый надёжный вариант)
            text += "🎬 <b>Смотреть онлайн:</b>\n"
            text += f"• <a href='https://www.kinopoisk.ru/series/{kp_id}/watch/'>Кинопоиск HD (попробовать)</a>\n"

        # Клавиатура остаётся
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("✅ Отметить просмотренные серии", callback_data=f"series_track:{int(kp_id)}"))
        
        # Отправка (твой остальной код)
        bot.send_message(
            chat_id,
            text,
            parse_mode='HTML',
            reply_markup=markup,
            disable_web_page_preview=False
        )
        
        logger.info(f"[SERIES NOTIFICATION] Уведомление отправлено chat_id={chat_id}, kp_id={kp_id}, s{season}e{episode}")
        
    except Exception as e:
        logger.error(f"[SERIES NOTIFICATION] Ошибка отправки: {e}", exc_info=True)
        seasons = get_seasons_data(kp_id)
        
        if seasons:
            now = datetime.now()
            next_episode_date = None
            next_episode = None
            
            for season in seasons:
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
                                if not next_episode_date or release_date < next_episode_date:
                                    next_episode_date = release_date
                                    next_episode = {
                                        'season': season.get('number', ''),
                                        'episode': ep.get('episodeNumber', ''),
                                        'date': release_date
                                    }
                        except:
                            pass
            
            if next_episode_date and next_episode:
                # Есть следующая серия - ставим уведомление и отправляем сообщение
                # Получаем часовой пояс пользователя
                user_tz = pytz.timezone('Europe/Moscow')
                try:
                    with db_lock:
                        cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'timezone'", (chat_id,))
                        tz_row = cursor.fetchone()
                        if tz_row:
                            tz_str = tz_row.get('value') if isinstance(tz_row, dict) else tz_row[0]
                            user_tz = pytz.timezone(tz_str)
                except:
                    pass
                
                # Уведомление за день до выхода
                notification_time = next_episode_date - timedelta(days=1)
                notification_time = user_tz.localize(notification_time.replace(hour=10, minute=0))
                
                # Ставим уведомление для каждого подписанного пользователя
                for user_id in subscribers_list:
                    scheduler.add_job(
                        send_series_notification,
                        'date',
                        run_date=notification_time.astimezone(pytz.utc),
                        args=[chat_id, film_id, kp_id, title, next_episode['season'], next_episode['episode']],
                        id=f'series_notification_{chat_id}_{film_id}_{user_id}_{next_episode_date.strftime("%Y%m%d")}'
                    )
                
                # Отправляем сообщение о следующей серии
                next_text = f"📅 <b>Следующая серия:</b>\n\n"
                next_text += f"📺 <b>{title}</b>\n"
                next_text += f"📅 Сезон {next_episode['season']}, Эпизод {next_episode['episode']} — {next_episode_date.strftime('%d.%m.%Y')}\n\n"
                next_text += f"✅ Уведомление установлено на {notification_time.strftime('%d.%m.%Y в %H:%M')}"
                
                try:
                    bot.send_message(chat_id, next_text, parse_mode='HTML')
                    logger.info(f"[SERIES NOTIFICATION] Сообщение о следующей серии отправлено для {title} (kp_id={kp_id})")
                except Exception as e:
                    logger.error(f"[SERIES NOTIFICATION] Ошибка отправки сообщения о следующей серии: {e}")
            else:
                # Нет следующей серии - ставим периодическую проверку
                check_time = dt.now(pytz.utc) + timedelta(weeks=3)
                for user_id in subscribers_list:
                    scheduler.add_job(
                        check_series_for_new_episodes,
                        'date',
                        run_date=check_time,
                        args=[chat_id, film_id, kp_id, user_id],
                        id=f'series_check_{chat_id}_{film_id}_{user_id}_{int(check_time.timestamp())}'
                    )
                logger.info(f"[SERIES NOTIFICATION] Следующая проверка через 3 недели для {title} (kp_id={kp_id})")
    except Exception as e:
        logger.error(f"[SERIES NOTIFICATION] Ошибка: {e}", exc_info=True)

def check_series_for_new_episodes(chat_id, film_id, kp_id, user_id):
    """Проверяет сериал на наличие новых серий и ставит уведомления"""
    try:
        if not bot or not scheduler:
            logger.error("[SERIES CHECK] bot или scheduler не установлен")
            return
        
        seasons = get_seasons_data(kp_id)
        
        if not seasons:
            logger.warning(f"[SERIES CHECK] Не удалось получить данные о сезонах для kp_id={kp_id}")
            return
        
        # Проверяем, подписан ли еще пользователь
        with db_lock:
            cursor.execute('SELECT subscribed FROM series_subscriptions WHERE chat_id = %s AND film_id = %s AND user_id = %s', (chat_id, film_id, user_id))
            sub_row = cursor.fetchone()
            is_subscribed = sub_row and (sub_row.get('subscribed') if isinstance(sub_row, dict) else sub_row[0])
        
        if not is_subscribed:
            logger.info(f"[SERIES CHECK] Пользователь {user_id} отписался от сериала kp_id={kp_id}")
            return
        
        # Ищем следующую серию
        now = datetime.now()
        next_episode_date = None
        next_episode = None
        
        for season in seasons:
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
                            if not next_episode_date or release_date < next_episode_date:
                                next_episode_date = release_date
                                next_episode = {
                                    'season': season.get('number', ''),
                                    'episode': ep.get('episodeNumber', ''),
                                    'date': release_date
                                }
                    except:
                        pass
        
        if next_episode_date and next_episode:
            # Есть ближайшая дата - ставим уведомление и отправляем сообщение
            
            # Получаем часовой пояс пользователя
            user_tz = pytz.timezone('Europe/Moscow')
            try:
                with db_lock:
                    cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'timezone'", (chat_id,))
                    tz_row = cursor.fetchone()
                    if tz_row:
                        tz_str = tz_row.get('value') if isinstance(tz_row, dict) else tz_row[0]
                        user_tz = pytz.timezone(tz_str)
            except:
                pass
            
            # Уведомление за день до выхода
            notification_time = next_episode_date - timedelta(days=1)
            notification_time = user_tz.localize(notification_time.replace(hour=10, minute=0))
            
            with db_lock:
                cursor.execute("SELECT title FROM movies WHERE id = %s", (film_id,))
                title_row = cursor.fetchone()
                title = title_row.get('title') if title_row and isinstance(title_row, dict) else (title_row[0] if title_row else "Сериал")
            
            scheduler.add_job(
                send_series_notification,
                'date',
                run_date=notification_time.astimezone(pytz.utc),
                args=[chat_id, film_id, kp_id, title, next_episode['season'], next_episode['episode']],
                id=f'series_notification_{chat_id}_{film_id}_{user_id}_{next_episode_date.strftime("%Y%m%d")}'
            )
            
            # Отправляем уведомление о найденной новой серии
            notification_text = f"🔔 <b>Найдена новая серия!</b>\n\n"
            notification_text += f"📺 <b>{title}</b>\n"
            notification_text += f"📅 Сезон {next_episode['season']}, Эпизод {next_episode['episode']} — {next_episode_date.strftime('%d.%m.%Y')}\n\n"
            notification_text += f"✅ Уведомление установлено на {notification_time.strftime('%d.%m.%Y в %H:%M')}\n\n"
            notification_text += f"<a href='https://www.kinopoisk.ru/series/{kp_id}/'>Кинопоиск</a>"
            
            try:
                bot.send_message(chat_id, notification_text, parse_mode='HTML')
                logger.info(f"[SERIES CHECK] Уведомление о новой серии отправлено для {title} (kp_id={kp_id})")
            except Exception as e:
                logger.error(f"[SERIES CHECK] Ошибка отправки уведомления: {e}")
            
            logger.info(f"[SERIES CHECK] Уведомление поставлено на {next_episode_date.strftime('%d.%m.%Y')} для сериала kp_id={kp_id}")
        else:
            # Нет ближайшей даты - ставим следующую проверку через 3 недели
            check_time = dt.now(pytz.utc) + timedelta(weeks=3)
            scheduler.add_job(
                check_series_for_new_episodes,
                'date',
                run_date=check_time,
                args=[chat_id, film_id, kp_id, user_id],
                id=f'series_check_{chat_id}_{film_id}_{user_id}_{int(check_time.timestamp())}'
            )
            logger.info(f"[SERIES CHECK] Следующая проверка через 3 недели для сериала kp_id={kp_id}")
    except Exception as e:
        logger.error(f"[SERIES CHECK] Ошибка: {e}", exc_info=True)



def send_rating_reminder(chat_id, film_id, film_title, user_id):
    """Отправляет напоминание пользователю об оценке фильма на следующий день после просмотра"""

    try:

        # Проверяем, не оценил ли уже пользователь

        with db_lock:

            cursor.execute("""

                SELECT id FROM ratings 

                WHERE chat_id = %s AND film_id = %s AND user_id = %s

            """, (chat_id, film_id, user_id))

            has_rating = cursor.fetchone()

            

            if has_rating:

                logger.info(f"[RATING REMINDER] Пользователь {user_id} уже оценил фильм {film_id}, пропускаем")

                return

            

            # Получаем ссылку на фильм

            cursor.execute("SELECT link FROM movies WHERE id = %s", (film_id,))

            film_row = cursor.fetchone()

            link = film_row.get('link') if isinstance(film_row, dict) else (film_row[0] if film_row else None)

            

            # Отправляем напоминание

            message_text = (

                f"📅 Напоминание: вы просмотрели фильм <b>{film_title}</b> вчера.\n\n"

                f"💬 Ответьте числом от 1 до 10 на это сообщение или на сообщение с фильмом, чтобы поставить оценку."

            )

            

            if link:

                message_text += f"\n\n{link}"

            

            msg = bot.send_message(chat_id, message_text, parse_mode='HTML')

            

            # Сохраняем связь для обработки оценки
            from moviebot.states import rating_messages
            rating_messages[msg.message_id] = film_id

            logger.info(f"[RATING REMINDER] Напоминание отправлено user_id={user_id}, film_id={film_id}, message_id={msg.message_id}")

    except Exception as e:

        logger.error(f"[RATING REMINDER] Ошибка при отправке напоминания: {e}", exc_info=True)


def check_subscription_payments():
    """Проверяет подписки и отправляет уведомления за день до списания"""
    if not bot:
        return
    
    try:
        from moviebot.database.db_operations import get_active_subscription        
        now = datetime.now(pytz.UTC)
        tomorrow = now + timedelta(days=1)
        
        # Находим подписки, у которых next_payment_date завтра
        with db_lock:
            cursor.execute("""
                SELECT id, chat_id, user_id, subscription_type, plan_type, period_type, price, next_payment_date
                FROM subscriptions
                WHERE is_active = TRUE
                AND next_payment_date IS NOT NULL
                AND DATE(next_payment_date AT TIME ZONE 'UTC') = DATE(%s AT TIME ZONE 'UTC')
            """, (tomorrow,))
            subscriptions = cursor.fetchall()
        
        for sub in subscriptions:
            try:
                subscription_id = sub.get('id') if isinstance(sub, dict) else sub[0]
                chat_id = sub.get('chat_id') if isinstance(sub, dict) else sub[1]
                user_id = sub.get('user_id') if isinstance(sub, dict) else sub[2]
                subscription_type = sub.get('subscription_type') if isinstance(sub, dict) else sub[3]
                plan_type = sub.get('plan_type') if isinstance(sub, dict) else sub[4]
                period_type = sub.get('period_type') if isinstance(sub, dict) else sub[5]
                price = sub.get('price') if isinstance(sub, dict) else sub[6]
                next_payment = sub.get('next_payment_date') if isinstance(sub, dict) else sub[7]
                
                plan_names = {
                    'notifications': '🔔 Уведомления о сериалах',
                    'recommendations': '🎯 Персональные рекомендации',
                    'tickets': '🎫 Билеты в кино',
                    'all': '📦 Все режимы'
                }
                
                period_names = {
                    'month': 'месяц',
                    '3months': '3 месяца',
                    'year': 'год',
                    'lifetime': 'навсегда'
                }
                
                plan_name = plan_names.get(plan_type, plan_type)
                period_name = period_names.get(period_type, period_type)
                
                text = "🔔 <b>Напоминание о списании</b>\n\n"
                text += f"Завтра ({next_payment.strftime('%d.%m.%Y') if isinstance(next_payment, datetime) else next_payment}) будет списана оплата за подписку:\n\n"
                if subscription_type == 'personal':
                    text += f"👤 Личная подписка\n"
                else:
                    text += f"👥 Групповая подписка\n"
                text += f"{plan_name}\n"
                text += f"⏰ Период: {period_name}\n"
                text += f"💰 Сумма: <b>{price}₽</b>\n\n"
                text += "💡 Вы можете изменить или отменить подписку до списания."
                
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("✅ Ок", callback_data=f"payment:reminder_ok:{subscription_id}"))
                markup.add(InlineKeyboardButton("✏️ Изменить подписку", callback_data=f"payment:modify:{subscription_id}"))
                markup.add(InlineKeyboardButton("❌ Отменить подписку", callback_data=f"payment:cancel:{subscription_id}"))
                
                # Для личных подписок отправляем в личку, для групповых - в групповой чат
                if subscription_type == 'personal':
                    # Отправляем в личку пользователю
                    bot.send_message(user_id, text, reply_markup=markup, parse_mode='HTML')
                    logger.info(f"[SUBSCRIPTION PAYMENT] Отправлено уведомление о списании в личку для подписки {subscription_id}, user_id={user_id}")
                else:
                    # Отправляем в групповой чат
                    bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
                    logger.info(f"[SUBSCRIPTION PAYMENT] Отправлено уведомление о списании в группу для подписки {subscription_id}, chat_id={chat_id}")
                
            except Exception as e:
                logger.error(f"[SUBSCRIPTION PAYMENT] Ошибка отправки уведомления для подписки: {e}", exc_info=True)
    
    except Exception as e:
        logger.error(f"[SUBSCRIPTION PAYMENT] Ошибка проверки подписок: {e}", exc_info=True)


def send_successful_payment_notification(chat_id, subscription_id, subscription_type, plan_type, period_type, is_recurring=False):
    """Отправляет уведомление об успешном платеже"""
    if not bot:
        return
    
    try:
        from moviebot.database.db_operations import get_subscription_by_id
        
        # Получаем информацию о подписке
        sub = get_subscription_by_id(subscription_id)
        if not sub:
            logger.error(f"[SUCCESSFUL PAYMENT] Подписка {subscription_id} не найдена")
            return
        
        expires_at = sub.get('expires_at')
        next_payment_date = sub.get('next_payment_date')
        
        plan_names = {
            'notifications': '🔔 Уведомления о сериалах',
            'recommendations': '🎯 Персональные рекомендации',
            'tickets': '🎫 Билеты в кино',
            'all': '📦 Все режимы'
        }
        plan_name = plan_names.get(plan_type, plan_type)
        
        # Определяем список функций для подписки
        features_list = []
        if plan_type == 'all':
            features_list = [
                '🔔 Уведомления о сериалах',
                '🎯 Персональные рекомендации',
                '🎫 Билеты в кино'
            ]
        elif plan_type == 'notifications':
            features_list = ['🔔 Уведомления о сериалах']
        elif plan_type == 'recommendations':
            features_list = ['🎯 Персональные рекомендации']
        elif plan_type == 'tickets':
            features_list = ['🎫 Билеты в кино']
        
        if is_recurring:
            # Уведомление для рекуррентных платежей
            text = "✅ <b>Спасибо, что вы с нами!</b>\n\n"
            text += f"Ваш план продлён до "
            
            # Показываем дату следующего списания (next_payment_date), а не expires_at
            if next_payment_date:
                if isinstance(next_payment_date, datetime):
                    next_payment_local = next_payment_date.astimezone(PLANS_TZ) if next_payment_date.tzinfo else PLANS_TZ.localize(next_payment_date)
                    text += f"<b>{next_payment_local.strftime('%d.%m.%Y')}</b>\n\n"
                else:
                    # Если next_payment_date - строка, пытаемся распарсить
                    try:
                        from dateutil import parser
                        next_payment_dt = parser.parse(str(next_payment_date))
                        next_payment_local = next_payment_dt.astimezone(PLANS_TZ) if next_payment_dt.tzinfo else PLANS_TZ.localize(next_payment_dt)
                        text += f"<b>{next_payment_local.strftime('%d.%m.%Y')}</b>\n\n"
                    except:
                        text += f"<b>{next_payment_date}</b>\n\n"
            elif period_type == 'lifetime' or expires_at is None:
                text += "<b>бессрочно</b>\n\n"
            else:
                # Если next_payment_date нет, используем expires_at
                if isinstance(expires_at, datetime):
                    expires_at_local = expires_at.astimezone(PLANS_TZ) if expires_at.tzinfo else PLANS_TZ.localize(expires_at)
                    text += f"<b>{expires_at_local.strftime('%d.%m.%Y')}</b>\n\n"
                else:
                    try:
                        from dateutil import parser
                        expires_at_dt = parser.parse(str(expires_at))
                        expires_at_local = expires_at_dt.astimezone(PLANS_TZ) if expires_at_dt.tzinfo else PLANS_TZ.localize(expires_at_dt)
                        text += f"<b>{expires_at_local.strftime('%d.%m.%Y')}</b>\n\n"
                    except:
                        text += f"<b>{expires_at}</b>\n\n"
            
            text += "Вам доступны:\n"
            for feature in features_list:
                text += f"• {feature}\n"
        else:
            # Уведомление для обычных платежей (старый формат)
            text = "✅ <b>Спасибо, оплата успешно проведена!</b>\n\n"
            text += f"Ваша подписка: {plan_name}\n"
            
            # Если подписка навсегда, показываем "Действует неограниченно"
            if period_type == 'lifetime' or expires_at is None:
                text += "Действует неограниченно"
            else:
                # Показываем дату окончания действия подписки
                if isinstance(expires_at, datetime):
                    expires_at_local = expires_at.astimezone(PLANS_TZ) if expires_at.tzinfo else PLANS_TZ.localize(expires_at)
                    text += f"Действует до: {expires_at_local.strftime('%d.%m.%Y')}"
                else:
                    # Если expires_at - строка, пытаемся распарсить
                    try:
                        from dateutil import parser
                        expires_at_dt = parser.parse(str(expires_at))
                        expires_at_local = expires_at_dt.astimezone(PLANS_TZ) if expires_at_dt.tzinfo else PLANS_TZ.localize(expires_at_dt)
                        text += f"Действует до: {expires_at_local.strftime('%d.%m.%Y')}"
                    except:
                        text += f"Действует до: {expires_at}"
        
        markup = InlineKeyboardMarkup()
        
        # Для групповых подписок проверяем, есть ли участники группы, которых можно добавить
        # Только для обычных платежей (не рекуррентных)
        if subscription_type == 'group' and chat_id < 0 and not is_recurring:
            try:
                from moviebot.database.db_operations import get_subscription_members, get_active_group_users
                from moviebot.bot.bot_init import BOT_ID
                
                # Получаем участников подписки и активных пользователей группы
                members = get_subscription_members(subscription_id)
                if BOT_ID and BOT_ID in members:
                    members = {uid: uname for uid, uname in members.items() if uid != BOT_ID}
                
                active_users = get_active_group_users(chat_id, BOT_ID)
                
                # Находим участников группы, которые не в подписке
                not_in_subscription = []
                for user_id, username in active_users.items():
                    if user_id not in members:
                        not_in_subscription.append({
                            'user_id': user_id,
                            'username': username
                        })
                
                # Если есть участники для добавления, предлагаем их добавить
                if not_in_subscription:
                    text += "\n\n"
                    text += "👥 <b>В вашей группе есть участники, которых можно добавить в подписку:</b>\n\n"
                    
                    # Добавляем кнопки для добавления участников (максимум 10)
                    for member in not_in_subscription[:10]:
                        display_name = member['username'] if member['username'].startswith('user_') else f"@{member['username']}"
                        button_text = f"➕ {display_name}"
                        if len(button_text) > 50:
                            button_text = button_text[:47] + "..."
                        markup.add(InlineKeyboardButton(button_text, callback_data=f"payment:add_member:{subscription_id}:{member['user_id']}"))
                    
                    if len(not_in_subscription) > 10:
                        text += f"\n... и еще {len(not_in_subscription) - 10} участник(ов)"
            except Exception as e:
                logger.error(f"[SUCCESSFUL PAYMENT] Ошибка при получении участников для добавления: {e}")
        
        markup.add(InlineKeyboardButton("✅ Готово", callback_data="payment:success_ok"))
        
        # Для личных подписок отправляем в личку пользователя, для групповых - в групповой чат
        if subscription_type == 'personal':
            target_chat_id = sub.get('user_id')
        else:
            # Для групповых подписок отправляем в группу
            target_chat_id = chat_id
        
        try:
            bot.send_message(target_chat_id, text, reply_markup=markup, parse_mode='HTML')
            logger.info(f"[SUCCESSFUL PAYMENT] Уведомление отправлено для подписки {subscription_id}")
        except Exception as e:
            logger.error(f"[SUCCESSFUL PAYMENT] Ошибка отправки уведомления: {e}")
    except Exception as e:
        logger.error(f"[SUCCESSFUL PAYMENT] Ошибка: {e}", exc_info=True)


def process_recurring_payments():
    """Выполняет безакцептные списания для подписок с payment_method_id"""
    if not bot:
        return
    
    try:
        from moviebot.api.yookassa_api import create_recurring_payment
        from moviebot.database.db_operations import renew_subscription, save_payment, update_payment_status, create_subscription
        import uuid as uuid_module
        
        now = datetime.now(pytz.UTC)
        
        # КРИТИЧЕСКИЙ ФИКС: Добавляем rollback при ошибках транзакции
        try:
            # Сначала делаем rollback на случай если предыдущая транзакция упала
            try:
                conn.rollback()
            except:
                pass
        except:
            pass
        
        # Находим подписки, у которых next_payment_date наступил и есть payment_method_id
        # Для тестовых подписок проверяем по времени (если прошло 10 минут)
        # Для остальных - только в дневное время (9:00 МСК)
        subscriptions = []
        with db_lock:
            try:
                # Для тестовых подписок проверяем, если next_payment_date <= now
                # Для остальных - только если сегодня и в дневное время (9:00-18:00 МСК)
                cursor.execute("""
                    SELECT id, chat_id, user_id, subscription_type, plan_type, period_type, price, 
                           next_payment_date, payment_method_id, telegram_username, group_username, group_size
                    FROM subscriptions
                    WHERE is_active = TRUE
                    AND next_payment_date IS NOT NULL
                    AND payment_method_id IS NOT NULL
                    AND period_type != 'lifetime'
                    AND (
                        -- Тестовые подписки: проверяем по времени (если прошло 10 минут)
                        (period_type = 'test' AND next_payment_date <= %s)
                        OR
                        -- Остальные подписки: только в дневное время (9:00-18:00 МСК) и сегодня
                        (period_type != 'test' 
                         AND DATE(next_payment_date AT TIME ZONE 'UTC') = DATE(%s AT TIME ZONE 'UTC')
                         AND EXTRACT(HOUR FROM (now() AT TIME ZONE 'Europe/Moscow')) >= 9
                         AND EXTRACT(HOUR FROM (now() AT TIME ZONE 'Europe/Moscow')) < 18)
                    )
                """, (now, now))
                subscriptions = cursor.fetchall()
            except Exception as db_e:
                logger.error(f"[RECURRING PAYMENT] Ошибка при запросе подписок: {db_e}", exc_info=True)
                try:
                    conn.rollback()
                except:
                    pass
                subscriptions = []
        
        for sub in subscriptions:
            try:
                subscription_id = sub.get('id') if isinstance(sub, dict) else sub[0]
                chat_id = sub.get('chat_id') if isinstance(sub, dict) else sub[1]
                user_id = sub.get('user_id') if isinstance(sub, dict) else sub[2]
                subscription_type = sub.get('subscription_type') if isinstance(sub, dict) else sub[3]
                plan_type = sub.get('plan_type') if isinstance(sub, dict) else sub[4]
                period_type = sub.get('period_type') if isinstance(sub, dict) else sub[5]
                # ВАЖНО: Используем цену из БД (сохраненную при создании подписки),
                # а НЕ текущую цену из SUBSCRIPTION_PRICES.
                # Это гарантирует, что изменение тарифов не повлияет на существующие подписки.
                price = float(sub.get('price') if isinstance(sub, dict) else sub[6])
                payment_method_id = sub.get('payment_method_id') if isinstance(sub, dict) else sub[8]
                telegram_username = sub.get('telegram_username') if isinstance(sub, dict) else sub[9]
                group_username = sub.get('group_username') if isinstance(sub, dict) else sub[10]
                group_size = sub.get('group_size') if isinstance(sub, dict) else sub[11]
                
                logger.info(f"[RECURRING PAYMENT] Обработка подписки {subscription_id}, payment_method_id={payment_method_id}, сумма={price} (из БД)")
                
                # Создаем безакцептный платеж используя сохраненный payment_method_id
                # ВАЖНО: Используем price из БД, а не из SUBSCRIPTION_PRICES
                payment = create_recurring_payment(
                    user_id=user_id,
                    chat_id=chat_id,
                    subscription_type=subscription_type,
                    plan_type=plan_type,
                    period_type=period_type,
                    amount=price,  # Цена из БД (сохраненная при создании подписки)
                    payment_method_id=payment_method_id,
                    group_size=group_size,
                    telegram_username=telegram_username,
                    group_username=group_username
                )
                
                if not payment:
                    logger.error(f"[RECURRING PAYMENT] Не удалось создать платеж для подписки {subscription_id}")
                    continue
                
                # Извлекаем payment_id из metadata платежа
                payment_id = None
                if hasattr(payment, 'metadata') and payment.metadata:
                    payment_id = payment.metadata.get('payment_id')
                if not payment_id:
                    payment_id = str(uuid_module.uuid4())
                
                logger.info(f"[RECURRING PAYMENT] Платеж создан: {payment.id}, статус: {payment.status}")
                
                period_names = {
                    'month': 'месяц',
                    '3months': '3 месяца',
                    'year': 'год',
                    'test': 'тестовый (10 минут)'
                }
                period_name = period_names.get(period_type, period_type)
                
                # Сохраняем платеж в БД
                save_payment(
                    payment_id=payment_id,
                    yookassa_payment_id=payment.id,
                    user_id=user_id,
                    chat_id=chat_id,
                    subscription_type=subscription_type,
                    plan_type=plan_type,
                    period_type=period_type,
                    group_size=group_size,
                    amount=price,
                    status=payment.status
                )
                
                # Если платеж успешен, продлеваем подписку
                if payment.status == 'succeeded':
                    renew_subscription(subscription_id, period_type)
                    update_payment_status(payment_id, 'succeeded', subscription_id)
                    
                    # Отправляем уведомление об успешном рекуррентном платеже
                    # is_recurring=True для отображения специального текста
                    send_successful_payment_notification(
                        chat_id=chat_id,
                        subscription_id=subscription_id,
                        subscription_type=subscription_type,
                        plan_type=plan_type,
                        period_type=period_type,
                        is_recurring=True
                    )
                else:
                    # Платеж не успешен - проверяем причину
                    has_cancellation_details = hasattr(payment, 'cancellation_details') and payment.cancellation_details
                    cancellation_reason = None
                    if has_cancellation_details:
                        cancellation_reason = getattr(payment.cancellation_details, 'reason', None) or \
                                             (payment.cancellation_details.get('reason') if isinstance(payment.cancellation_details, dict) else None)
                    
                    logger.warning(f"[RECURRING PAYMENT] Платеж {payment.id} не успешен, статус: {payment.status}, cancellation_details: {has_cancellation_details}, reason: {cancellation_reason}")
                    
                    # Подсчитываем количество неудачных попыток за последние 7 дней
                    from moviebot.database.db_connection import get_db_connection, get_db_cursor
                    conn_retry = get_db_connection()
                    cursor_retry = conn_retry.cursor()
                    retry_count = 0
                    try:
                        seven_days_ago = now - timedelta(days=7)
                        with db_lock:
                            cursor_retry.execute("""
                                SELECT COUNT(*) 
                                FROM payments 
                                WHERE subscription_id = %s 
                                AND status IN ('canceled', 'pending', 'waiting_for_capture')
                                AND created_at >= %s
                            """, (subscription_id, seven_days_ago))
                            retry_count_result = cursor_retry.fetchone()
                            retry_count = retry_count_result[0] if retry_count_result and isinstance(retry_count_result, tuple) else \
                                         (retry_count_result.get('count') if isinstance(retry_count_result, dict) else 0)
                    except Exception as e:
                        logger.error(f"[RECURRING PAYMENT] Ошибка подсчета попыток: {e}")
                    
                    # Если есть cancellation_details или это одна из первых 5 попыток - планируем повтор
                    if has_cancellation_details and retry_count < 5:
                        # ПЛАНИРУЕМ ПОВТОРНУЮ ПОПЫТКУ ЧЕРЕЗ ДЕНЬ
                        # Для неудачных рекуррентных платежей следующая попытка будет через день в 9:00 МСК
                        tomorrow = now + timedelta(days=1)
                        next_attempt = PLANS_TZ.localize(
                            datetime.combine(tomorrow.date(), datetime.min.time().replace(hour=9, minute=0))
                        ).astimezone(pytz.UTC)
                        
                        logger.info(f"[RECURRING PAYMENT] Планируется повторная попытка для подписки {subscription_id} через день: {next_attempt}")
                        
                        # Обновляем next_payment_date в БД
                        from moviebot.database.db_operations import update_subscription_next_payment
                        update_subscription_next_payment(subscription_id, next_attempt)
                        
                        # Отправляем уведомление об ошибке с кнопками
                        text = "🚨 <b>Оплата не прошла!</b>\n\n"
                        if retry_count < 4:
                            text += f"Попытка {retry_count + 1} из 5. Следующая попытка списания будет через день. Пожалуйста, обеспечьте наличие средств на карте."
                        else:
                            text += f"Попытка {retry_count + 1} из 5. Это последняя автоматическая попытка. Если списание не пройдет, подписка будет приостановлена."
                        text += "\n\nВы также можете инициировать списание по кнопке ниже."
                        
                        markup = InlineKeyboardMarkup(row_width=1)
                        markup.add(InlineKeyboardButton("Провести платеж", callback_data=f"payment:retry_payment:{subscription_id}"))
                        markup.add(InlineKeyboardButton("Изменить тариф", callback_data=f"payment:modify:{subscription_id}"))
                        markup.add(InlineKeyboardButton("Отменить подписку", callback_data=f"payment:cancel:{subscription_id}"))
                        
                        # Для личных подписок отправляем в личку, для групповых - в групповой чат
                        target_chat_id = user_id if subscription_type == 'personal' else chat_id
                        try:
                            bot.send_message(target_chat_id, text, reply_markup=markup, parse_mode='HTML')
                            logger.info(f"[RECURRING PAYMENT] Уведомление об ошибке отправлено для подписки {subscription_id}, попытка {retry_count + 1}/5, следующая попытка: {next_attempt}")
                        except Exception as e:
                            logger.error(f"[RECURRING PAYMENT] Ошибка отправки уведомления об ошибке: {e}")
                    else:
                        # Превышен лимит попыток или нет cancellation_details - отключаем автоплатежи
                        if retry_count >= 5:
                            logger.warning(f"[RECURRING PAYMENT] Превышен лимит попыток (5) для подписки {subscription_id}, отключаем автоплатежи")
                            # Обнуляем payment_method_id, чтобы прекратить автоплатежи
                            with db_lock:
                                cursor_retry.execute("""
                                    UPDATE subscriptions 
                                    SET payment_method_id = NULL
                                    WHERE id = %s
                                """, (subscription_id,))
                                conn_retry.commit()
                            
                            # Отправляем уведомление о приостановке автоплатежей
                            text = "⛔ <b>Автоплатежи приостановлены</b>\n\n"
                            text += "После 5 неудачных попыток автоплатежи были приостановлены. Вы можете возобновить подписку, оплатив её вручную."
                            
                            markup = InlineKeyboardMarkup(row_width=1)
                            markup.add(InlineKeyboardButton("Оплатить подписку", callback_data="payment:tariffs"))
                            markup.add(InlineKeyboardButton("Отменить подписку", callback_data=f"payment:cancel:{subscription_id}"))
                            
                            target_chat_id = user_id if subscription_type == 'personal' else chat_id
                            try:
                                bot.send_message(target_chat_id, text, reply_markup=markup, parse_mode='HTML')
                            except Exception as e:
                                logger.error(f"[RECURRING PAYMENT] Ошибка отправки уведомления о приостановке: {e}")
                
            except Exception as e:
                logger.error(f"[RECURRING PAYMENT] Ошибка обработки подписки {subscription_id}: {e}", exc_info=True)
    
    except Exception as e:
        logger.error(f"[RECURRING PAYMENT] Ошибка обработки рекуррентных платежей: {e}", exc_info=True)


def get_random_events_enabled(chat_id):
    """Проверяет, включены ли случайные события для чата"""
    with db_lock:
        cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'random_events_enabled'", (chat_id,))
        row = cursor.fetchone()
        if row:
            value = row.get('value') if isinstance(row, dict) else row[0]
            return value == 'true'
    return True  # По умолчанию включено


def was_event_sent_today(chat_id, event_type):
    """Проверяет, было ли отправлено событие/уведомление сегодня для данного чата"""
    if not bot:
        return False
    try:
        now = datetime.now(PLANS_TZ)
        today = now.date()
        with db_lock:
            cursor.execute("""
                SELECT id FROM event_notifications 
                WHERE chat_id = %s AND event_type = %s AND sent_date = %s
            """, (chat_id, event_type, today))
            row = cursor.fetchone()
            return row is not None
    except Exception as e:
        logger.error(f"[EVENT NOTIFICATIONS] Ошибка проверки события: {e}", exc_info=True)
        return False


def mark_event_sent(chat_id, event_type):
    """Отмечает, что событие/уведомление было отправлено сегодня"""
    if not bot:
        return
    try:
        now = datetime.now(PLANS_TZ)
        today = now.date()
        with db_lock:
            cursor.execute("""
                INSERT INTO event_notifications (chat_id, event_type, sent_date)
                VALUES (%s, %s, %s)
                ON CONFLICT (chat_id, event_type, sent_date) DO NOTHING
            """, (chat_id, event_type, today))
            conn.commit()
    except Exception as e:
        logger.error(f"[EVENT NOTIFICATIONS] Ошибка сохранения события: {e}", exc_info=True)


def check_weekend_schedule():
    """Проверяет расписание на выходные (пт-сб-вс) и предлагает рандомный фильм, если нет планов домашнего просмотра.
    Выполняется только в пятницу. Если нет планов вообще (ни дома, ни в кино), отправляет уведомление раз в неделю."""
    if not bot:
        return
    try:
        now = datetime.now(PLANS_TZ)
        current_weekday = now.weekday()
        
        # Проверяем только в пятницу (4 = пятница)
        if current_weekday != 4:
            return
        
        # Получаем все групповые чаты
        with db_lock:
            cursor.execute("SELECT DISTINCT chat_id FROM movies")
            chat_rows = cursor.fetchall()
        
        for row in chat_rows:
            chat_id = row.get('chat_id') if isinstance(row, dict) else row[0]
            
            # Проверяем, что это групповой чат (не личный)
            try:
                chat_info = bot.get_chat(chat_id)
                if chat_info.type == 'private':
                    continue  # Пропускаем личные чаты
            except Exception as e:
                logger.warning(f"[WEEKEND SCHEDULE] Не удалось получить информацию о чате {chat_id}: {e}")
                continue
            
            # Проверяем, включены ли случайные события
            if not get_random_events_enabled(chat_id):
                continue
            
            # Проверяем, было ли уже отправлено какое-то событие/уведомление сегодня
            if was_event_sent_today(chat_id, 'random_event') or was_event_sent_today(chat_id, 'weekend_reminder') or was_event_sent_today(chat_id, 'premiere_reminder'):
                logger.info(f"[WEEKEND SCHEDULE] Пропуск чата {chat_id} - уже было отправлено событие сегодня")
                continue
            
            # Проверяем, отключено ли это напоминание
            cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'reminder_weekend_films_disabled'", (chat_id,))
            reminder_disabled_row = cursor.fetchone()
            if reminder_disabled_row:
                is_disabled = reminder_disabled_row.get('value') if isinstance(reminder_disabled_row, dict) else reminder_disabled_row[0]
                if is_disabled == 'true':
                    continue
            
            # Проверяем, есть ли планы на выходные (пт-сб-вс) для домашнего просмотра
            friday = now.replace(hour=0, minute=0, second=0, microsecond=0)
            sunday = now.replace(hour=23, minute=59, second=59, microsecond=0) + timedelta(days=2)
            
            # Проверяем планы домашнего просмотра на выходные
            cursor.execute('''
                SELECT COUNT(*) FROM plans
                WHERE chat_id = %s 
                AND plan_type = 'home'
                AND plan_datetime >= %s 
                AND plan_datetime <= %s
            ''', (chat_id, friday, sunday))
            home_plans_count = cursor.fetchone()
            home_count = home_plans_count.get('count') if isinstance(home_plans_count, dict) else home_plans_count[0] if home_plans_count else 0
            
            # Проверяем планы в кино на выходные
            cursor.execute('''
                SELECT COUNT(*) FROM plans
                WHERE chat_id = %s 
                AND plan_type = 'cinema'
                AND plan_datetime >= %s 
                AND plan_datetime <= %s
            ''', (chat_id, friday, sunday))
            cinema_plans_count = cursor.fetchone()
            cinema_count = cinema_plans_count.get('count') if isinstance(cinema_plans_count, dict) else cinema_plans_count[0] if cinema_plans_count else 0
            
            # Если нет планов домашнего просмотра, отправляем уведомление
            if home_count == 0:
                # Проверяем, когда последний раз отправляли уведомление о выходных
                cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'last_weekend_reminder_date'", (chat_id,))
                last_date_row = cursor.fetchone()
                
                should_send = True
                if last_date_row:
                    last_date_str = last_date_row.get('value') if isinstance(last_date_row, dict) else last_date_row[0]
                    try:
                        last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
                        days_passed = (now.date() - last_date).days
                        # Если нет планов вообще (ни дома, ни в кино), отправляем раз в неделю
                        if cinema_count == 0 and days_passed < 7:
                            should_send = False
                    except:
                        pass
                
                if should_send:
                    try:
                        markup = InlineKeyboardMarkup(row_width=1)
                        markup.add(InlineKeyboardButton("🎲 Найти фильм", callback_data="rand_final:go"))
                        markup.add(InlineKeyboardButton("⏰ Настройки напоминаний", callback_data="settings:notifications"))
                        markup.add(InlineKeyboardButton("❌ Отменить такие уведомления", callback_data="reminder:disable:weekend_films"))
                        
                        text = "🎬 На выходных нет запланированных фильмов для домашнего просмотра!\n\n"
                        if cinema_count == 0:
                            text += "Также нет планов похода в кино.\n\n"
                        text += "Хотите выбрать какой-нибудь фильм из вашей базы?"
                        
                        bot.send_message(
                            chat_id,
                            text,
                            reply_markup=markup,
                            parse_mode='HTML'
                        )
                        
                        # Отмечаем, что событие отправлено
                        mark_event_sent(chat_id, 'weekend_reminder')
                        
                        # Сохраняем дату последнего уведомления
                        cursor.execute('''
                            INSERT INTO settings (chat_id, key, value)
                            VALUES (%s, 'last_weekend_reminder_date', %s)
                            ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                        ''', (chat_id, now.date().isoformat()))
                        conn.commit()
                        
                        logger.info(f"[WEEKEND SCHEDULE] Отправлено уведомление о выходных для чата {chat_id}")
                    except Exception as e:
                        logger.error(f"[WEEKEND SCHEDULE] Ошибка при отправке уведомления: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[WEEKEND SCHEDULE] Ошибка в check_weekend_schedule: {e}", exc_info=True)


def check_premiere_reminder():
    """Проверяет, нет ли планов по премьерам, и отправляет напоминание.
    Выполняется только в пятницу. Если нет планов вообще (ни дома, ни в кино), отправляет раз в неделю."""
    if not bot:
        return
    try:
        now = datetime.now(PLANS_TZ)
        current_weekday = now.weekday()
        
        # Проверяем только в пятницу (4 = пятница)
        if current_weekday != 4:
            return
        
        # Получаем все групповые чаты
        with db_lock:
            cursor.execute("SELECT DISTINCT chat_id FROM movies")
            chat_rows = cursor.fetchall()
        
        for row in chat_rows:
            chat_id = row.get('chat_id') if isinstance(row, dict) else row[0]
            
            # Проверяем, что это групповой чат (не личный)
            try:
                chat_info = bot.get_chat(chat_id)
                if chat_info.type == 'private':
                    continue  # Пропускаем личные чаты
            except Exception as e:
                logger.warning(f"[PREMIERE REMINDER] Не удалось получить информацию о чате {chat_id}: {e}")
                continue
            
            # Проверяем, включены ли случайные события
            if not get_random_events_enabled(chat_id):
                continue
            
            # Проверяем, было ли уже отправлено какое-то событие/уведомление сегодня
            if was_event_sent_today(chat_id, 'random_event') or was_event_sent_today(chat_id, 'weekend_reminder') or was_event_sent_today(chat_id, 'premiere_reminder'):
                logger.info(f"[PREMIERE REMINDER] Пропуск чата {chat_id} - уже было отправлено событие сегодня")
                continue
            
            # Проверяем, отключено ли это напоминание
            cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'reminder_cinema_premieres_disabled'", (chat_id,))
            reminder_disabled_row = cursor.fetchone()
            if reminder_disabled_row:
                is_disabled = reminder_disabled_row.get('value') if isinstance(reminder_disabled_row, dict) else reminder_disabled_row[0]
                if is_disabled == 'true':
                    continue
            
            # Проверяем, когда последний раз добавляли фильм в кино (plan_type='cinema')
            cursor.execute('''
                SELECT MAX(plan_datetime) FROM plans
                WHERE chat_id = %s AND plan_type = 'cinema'
            ''', (chat_id,))
            last_cinema_row = cursor.fetchone()
            
            has_recent_cinema_plan = False
            if last_cinema_row:
                last_cinema = last_cinema_row.get('max') if isinstance(last_cinema_row, dict) else last_cinema_row[0]
                if last_cinema:
                    if isinstance(last_cinema, str):
                        last_cinema = datetime.fromisoformat(last_cinema.replace('Z', '+00:00'))
                    if last_cinema.tzinfo is None:
                        last_cinema = pytz.utc.localize(last_cinema)
                    last_cinema = last_cinema.astimezone(PLANS_TZ)
                    
                    if (now - last_cinema).days < 14:
                        has_recent_cinema_plan = True
            
            # Если давно не добавляли фильмы в кино (14+ дней), отправляем напоминание
            if not has_recent_cinema_plan:
                # Проверяем, когда последний раз отправляли напоминание
                cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'last_cinema_reminder_date'", (chat_id,))
                last_reminder_row = cursor.fetchone()
                
                should_send = True
                if last_reminder_row:
                    last_reminder_str = last_reminder_row.get('value') if isinstance(last_reminder_row, dict) else last_reminder_row[0]
                    try:
                        last_reminder = datetime.strptime(last_reminder_str, '%Y-%m-%d').date()
                        days_passed = (now.date() - last_reminder).days
                        # Если нет планов вообще (ни дома, ни в кино), отправляем раз в неделю
                        # Проверяем планы домашнего просмотра
                        friday = now.replace(hour=0, minute=0, second=0, microsecond=0)
                        sunday = now.replace(hour=23, minute=59, second=59, microsecond=0) + timedelta(days=2)
                        cursor.execute('''
                            SELECT COUNT(*) FROM plans
                            WHERE chat_id = %s 
                            AND plan_type = 'home'
                            AND plan_datetime >= %s 
                            AND plan_datetime <= %s
                        ''', (chat_id, friday, sunday))
                        home_plans_count = cursor.fetchone()
                        home_count = home_plans_count.get('count') if isinstance(home_plans_count, dict) else home_plans_count[0] if home_plans_count else 0
                        
                        if home_count == 0 and days_passed < 7:
                            should_send = False
                    except:
                        pass
                
                if should_send:
                    try:
                        from moviebot.api.kinopoisk_api import get_premieres_for_period
                        
                        # Получаем премьеры текущего месяца
                        premieres = get_premieres_for_period('current_month')
                        
                        if premieres:
                            text = "🎬 Вы давно ничего не добавляли к просмотру в кинотеатре! Посмотрите, что сейчас идет в кино:\n\n"
                            
                            # Формируем список премьер (первые 10)
                            for i, p in enumerate(premieres[:10], 1):
                                title = p.get('nameRu') or p.get('nameOriginal') or 'Без названия'
                                year = p.get('year') or ''
                                text += f"{i}. {title}"
                                if year:
                                    text += f" ({year})"
                                text += "\n"
                            
                            if len(premieres) > 10:
                                text += f"\n... и еще {len(premieres) - 10} премьер"
                            
                            text += "\n\nИспользуйте /premieres для просмотра всех премьер"
                            
                            markup = InlineKeyboardMarkup(row_width=1)
                            markup.add(InlineKeyboardButton("📅 Премьеры", callback_data="start_menu:premieres"))
                            markup.add(InlineKeyboardButton("⏰ Настройки напоминаний", callback_data="settings:notifications"))
                            markup.add(InlineKeyboardButton("❌ Отменить такие уведомления", callback_data="reminder:disable:cinema_premieres"))
                            
                            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
                            
                            # Отмечаем, что событие отправлено
                            mark_event_sent(chat_id, 'premiere_reminder')
                            
                            # Сохраняем дату последнего напоминания
                            cursor.execute('''
                                INSERT INTO settings (chat_id, key, value)
                                VALUES (%s, 'last_cinema_reminder_date', %s)
                                ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                            ''', (chat_id, now.date().isoformat()))
                            conn.commit()
                            
                            logger.info(f"[PREMIERE REMINDER] Отправлено напоминание о премьерах для чата {chat_id}")
                    except Exception as e:
                        logger.error(f"[PREMIERE REMINDER] Ошибка при отправке напоминания: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[PREMIERE REMINDER] Ошибка в check_premiere_reminder: {e}", exc_info=True)


def choose_random_participant():
    """Раз в две недели выбирает случайного участника для выбора фильма"""
    if not bot:
        return
    
    try:
        now = datetime.now(PLANS_TZ)
        
        # Получаем все групповые чаты
        with db_lock:
            cursor.execute("SELECT DISTINCT chat_id FROM movies")
            chat_rows = cursor.fetchall()
        
        for row in chat_rows:
            chat_id = row.get('chat_id') if isinstance(row, dict) else row[0]
            
            # Проверяем, что это групповой чат
            try:
                chat_info = bot.get_chat(chat_id)
                if chat_info.type == 'private':
                    continue
            except Exception as e:
                logger.warning(f"[RANDOM PARTICIPANT] Не удалось получить информацию о чате {chat_id}: {e}")
                continue
            
            if not get_random_events_enabled(chat_id):
                continue
            
            # Пропускаем, если сегодня уже было какое-то событие
            if was_event_sent_today(chat_id, 'random_event') or \
               was_event_sent_today(chat_id, 'weekend_reminder') or \
               was_event_sent_today(chat_id, 'premiere_reminder'):
                logger.info(f"[RANDOM PARTICIPANT] Пропуск чата {chat_id} - уже было отправлено событие сегодня")
                continue
            
            # Проверяем, когда последний раз выбирали участника
            cursor.execute(
                "SELECT value FROM settings WHERE chat_id = %s AND key = 'last_random_participant_date'",
                (chat_id,)
            )
            last_date_row = cursor.fetchone()
            
            if last_date_row:
                last_date_str = last_date_row.get('value') if isinstance(last_date_row, dict) else last_date_row[0]
                try:
                    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
                    if (now.date() - last_date).days < 14:
                        continue
                except:
                    pass
            
            # Получаем участников
            from moviebot.bot.bot_init import BOT_ID
            current_bot_id = BOT_ID
            if current_bot_id is None:
                try:
                    current_bot_id = bot.get_me().id
                except:
                    current_bot_id = None
            
            query = '''
                SELECT DISTINCT user_id, username 
                FROM stats 
                WHERE chat_id = %s 
                AND timestamp >= %s
            '''
            params = (chat_id, (now - timedelta(days=30)).isoformat())
            
            if current_bot_id:
                query += " AND user_id != %s"
                params += (current_bot_id,)
                
            cursor.execute(query, params)
            participants = cursor.fetchall()
            
            if not participants:
                continue
            
            # Проверка недели участия
            cursor.execute('''
                SELECT user_id, MIN(timestamp) as first_participation
                FROM stats
                WHERE chat_id = %s
                GROUP BY user_id
            ''', (chat_id,))
            first_participations = {
                row.get('user_id') if isinstance(row, dict) else row[0]:
                row.get('first_participation') if isinstance(row, dict) else row[1]
                for row in cursor.fetchall()
            }
            
            week_ago = now - timedelta(days=7)
            all_participated_week_ago = True
            for participant in participants:
                user_id = participant.get('user_id') if isinstance(participant, dict) else participant[0]
                fp = first_participations.get(user_id)
                if fp:
                    if isinstance(fp, str):
                        fp = datetime.fromisoformat(fp.replace('Z', '+00:00'))
                    if fp > week_ago:
                        all_participated_week_ago = False
                        break
            
            if not all_participated_week_ago:
                logger.info(f"[RANDOM PARTICIPANT] Пропуск чата {chat_id} - не прошла неделя с начала участия всех")
                continue
            
            # Выбираем участника
            participant = random.choice(participants)
            user_id = participant.get('user_id') if isinstance(participant, dict) else participant[0]
            username = participant.get('username') if isinstance(participant, dict) else participant[1]
            
            # Формируем имя
            if username:
                user_name = f"@{username}"
            else:
                try:
                    member = bot.get_chat_member(chat_id, user_id)
                    user_name = member.user.first_name or "участник"
                except:
                    user_name = "участник"
            
            # Готовим сообщение
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("🎲 Найти фильм", callback_data="rand_final:go"))
            markup.add(InlineKeyboardButton("❌ Отменить такие уведомления", callback_data="reminder:disable:random_events"))
            markup.add(InlineKeyboardButton("❌ Закрыть", callback_data="random_event:close"))
            
            text = "🔮 Вас посетил дух выбора случайного фильма!\n\n"
            text += f"Он выбрал <b>{user_name}</b> для выбора фильма для вашей компании."
            
            # Пытаемся отправить
            original_chat_id = chat_id
            sent = False
            
            for attempt in range(2):  # максимум 2 попытки
                try:
                    bot.send_message(
                        chat_id=chat_id,
                        text=text,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                    sent = True
                    break
                
                except ApiTelegramException as api_err:
                    if "group chat was upgraded to a supergroup chat" in str(api_err):
                        try:
                            new_chat_id = api_err.result_json['parameters']['migrate_to_chat_id']
                            logger.warning(f"[RANDOM PARTICIPANT] Миграция чата! {original_chat_id} → {new_chat_id}")
                            
                            # Обновляем chat_id во всех нужных местах
                            with db_lock:
                                tables_to_update = ['movies', 'stats', 'settings', 'events', 'reminders']
                                for table in tables_to_update:
                                    cursor.execute(f"""
                                        UPDATE {table}
                                        SET chat_id = %s
                                        WHERE chat_id = %s
                                    """, (new_chat_id, original_chat_id))
                                
                                # Также можно обновить другие таблицы, если они есть
                                conn.commit()
                            
                            logger.info(f"[RANDOM PARTICIPANT] Обновлено {cursor.rowcount} записей chat_id")
                            
                            # Меняем chat_id для повторной попытки
                            chat_id = new_chat_id
                            
                            # Даём Telegram секунду на обработку миграции
                            import time
                            time.sleep(1.5)
                            
                        except Exception as update_err:
                            logger.error(f"Ошибка при обновлении chat_id после миграции: {update_err}", exc_info=True)
                            break
                    else:
                        raise
                
                except Exception as e:
                    logger.error(f"[RANDOM PARTICIPANT] Ошибка отправки в {chat_id}: {e}", exc_info=True)
                    break
            
            if sent:
                # Отмечаем события
                mark_event_sent(chat_id, 'random_event')
                
                # Сохраняем дату
                with db_lock:
                    cursor.execute('''
                        INSERT INTO settings (chat_id, key, value)
                        VALUES (%s, 'last_random_participant_date', %s)
                        ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                    ''', (chat_id, now.date().isoformat()))
                    conn.commit()
                
                logger.info(f"[RANDOM PARTICIPANT] Успешно выбран участник {user_id} для чата {chat_id}")
            else:
                logger.warning(f"[RANDOM PARTICIPANT] Не удалось отправить сообщение в чат {original_chat_id}")
                
    except Exception as e:
        logger.error(f"[RANDOM PARTICIPANT] Глобальная ошибка в choose_random_participant: {e}", exc_info=True)


def start_dice_game():
    """Раз в две недели запускает игру в кубик для выбора фильма"""
    if not bot:
        return
    try:
        now = datetime.now(PLANS_TZ)
        
        # Получаем все групповые чаты
        with db_lock:
            cursor.execute("SELECT DISTINCT chat_id FROM movies")
            chat_rows = cursor.fetchall()
        
        for row in chat_rows:
            chat_id = row.get('chat_id') if isinstance(row, dict) else row[0]
            
            # Проверяем, что это групповой чат (не личный)
            try:
                chat_info = bot.get_chat(chat_id)
                if chat_info.type == 'private':
                    continue
            except Exception as e:
                logger.warning(f"[DICE GAME] Не удалось получить информацию о чате {chat_id}: {e}")
                continue
            
            # Проверяем, включены ли случайные события
            if not get_random_events_enabled(chat_id):
                continue
            
            # Проверяем, было ли уже отправлено какое-то событие сегодня
            if was_event_sent_today(chat_id, 'random_event') or \
               was_event_sent_today(chat_id, 'weekend_reminder') or \
               was_event_sent_today(chat_id, 'premiere_reminder'):
                logger.info(f"[DICE GAME] Пропуск чата {chat_id} - уже было отправлено событие сегодня")
                continue
            
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
                        continue
                except:
                    pass
            
            # Получаем список активных участников (за последние 30 дней)
            with db_lock:
                cursor.execute('''
                    SELECT DISTINCT user_id, username 
                    FROM stats 
                    WHERE chat_id = %s 
                    AND timestamp >= %s
                ''', (chat_id, (now - timedelta(days=30)).isoformat()))
                participants = cursor.fetchall()
            
            if len(participants) < 2:
                continue
            
            # ===== Отправка сообщения с кнопкой =====
            from moviebot.states import dice_game_state
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("🎲 Бросить кубик", callback_data="dice_game:start"))
            markup.add(InlineKeyboardButton("❌ Отменить такие уведомления", callback_data="reminder:disable:random_events"))
            markup.add(InlineKeyboardButton("❌ Закрыть", callback_data="random_event:close"))
            
            text = "🔮 Вас посетил дух выбора случайного фильма!\n\n"
            text += "Испытайте удачу и определите, кто выберет фильм для вашей компании."
            
            current_chat_id = chat_id  # будем использовать эту переменную для возможного обновления после миграции
            
            try:
                msg = bot.send_message(
                    chat_id=current_chat_id,
                    text=text,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                
                success = True
                
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
                        success = True
                        
                    except Exception as e2:
                        logger.error(f"[DICE GAME] Не удалось отправить сообщение даже в новый чат {new_chat_id}: {e2}", exc_info=True)
                        continue
                else:
                    logger.error(f"[DICE GAME] Ошибка Telegram API при отправке в чат {chat_id}: {e}", exc_info=True)
                    continue
                    
            except Exception as e:
                logger.error(f"[DICE GAME] Непредвиденная ошибка при отправке в чат {chat_id}: {e}", exc_info=True)
                continue
            
            else:
                success = True
            
            if not success:
                continue
                
            # ===== Сохраняем состояние игры (с учётом возможной миграции) =====
            dice_game_state[current_chat_id] = {
                'participants': {},           # сюда будут добавляться пользователи после броска
                'message_id': msg.message_id,  # сообщение с кнопкой
                'start_time': now,
                'dice_messages': {}            # message_id кубика → user_id
            }
            
            # Отмечаем, что событие отправлено
            mark_event_sent(chat_id if current_chat_id == chat_id else current_chat_id, 'random_event')
            
            # Сохраняем дату последнего запуска (по актуальному chat_id)
            with db_lock:
                cursor.execute('''
                    INSERT INTO settings (chat_id, key, value)
                    VALUES (%s, 'last_dice_game_date', %s)
                    ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                ''', (current_chat_id, now.date().isoformat()))
                conn.commit()
            
            logger.info(f"[DICE GAME] Запущена игра в кубик для чата {current_chat_id}")
            
    except Exception as e:
        logger.error(f"[DICE GAME] Критическая ошибка в start_dice_game: {e}", exc_info=True)

def update_series_status_cache():
    """Фоновая задача: обновляет статусы сериалов раз в день"""
    logger.info("[CACHE] Запуск обновления кэша сериалов")
    
    with db_lock:
        cursor.execute("""
            SELECT DISTINCT kp_id, chat_id
            FROM movies
            WHERE is_series = 1
              AND (last_api_update IS NULL OR last_api_update < NOW() - INTERVAL '1 day')
            LIMIT 30
        """)
        rows = cursor.fetchall()

    if not rows:
        logger.info("[CACHE] Нет сериалов для обновления кэша")
        return

    for row in rows:
        # Универсальная обработка: row может быть tuple или dict
        if isinstance(row, dict):
            kp_id = row.get('kp_id')
            chat_id = row.get('chat_id')
        else:
            # Предполагаем порядок из SELECT: kp_id, chat_id
            if len(row) < 2:
                logger.warning(f"[CACHE] Пропущена битая запись (слишком короткая): {row}")
                continue
            kp_id = row[0]
            chat_id = row[1]
        
        if kp_id is None:
            logger.warning(f"[CACHE] Пропущена запись с kp_id=None: {row}")
            continue
        
        try:
            # Основная логика: получаем актуальные данные из Kinopoisk API
            is_airing, next_ep = get_series_airing_status(kp_id)
            seasons_data = get_seasons_data(kp_id)
            seasons_count = len(seasons_data) if seasons_data else 0
            next_ep_json = json.dumps(next_ep) if next_ep else None

            # Обновляем только нужные колонки
            with db_lock:
                cursor.execute("""
                    UPDATE movies
                    SET is_ongoing = %s, 
                        seasons_count = %s, 
                        next_episode = %s, 
                        last_api_update = NOW()
                    WHERE chat_id = %s AND kp_id = %s
                """, (is_airing, seasons_count, next_ep_json, chat_id, kp_id))
                conn.commit()
            
            logger.info(f"[CACHE] Обновлён кэш для kp_id={kp_id} (chat_id={chat_id}), seasons={seasons_count}, ongoing={is_airing}")

        except Exception as e:
            logger.error(f"[CACHE] Ошибка обновления kp_id={kp_id} (chat_id={chat_id}): {e}", exc_info=True)

    logger.info("[CACHE] Обновление кэша сериалов завершено")