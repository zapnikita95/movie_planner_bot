"""
Модуль для задач планировщика
"""
# 1. Стандартная библиотека Python
import json
import logging
import random
import time
import pytz

from datetime import datetime, timedelta, date
from typing import Optional

# 2. Сторонние библиотеки (в алфавитном порядке)
import telebot
from telebot.apihelper import ApiTelegramException
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

import psycopg2
from psycopg2.extras import RealDictCursor
from moviebot.config import DATABASE_URL

# 3. APScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

# 4. Твои локальные импорты (отсортируй по алфавиту внутри группы)
from moviebot.bot.bot_init import bot, BOT_ID
from moviebot.database.db_connection import db_lock  # Только db_lock, get_db_connection убрали
from moviebot.config import PLANS_TZ
from moviebot.api.kinopoisk_api import get_seasons_data
from moviebot.api.kinopoisk_api import get_external_sources

# Импорт helpers отключён полностью — все нужные функции определены в этом же файле (scheduler.py)
# from moviebot.utils.helpers import (...)
from moviebot.database.db_operations import get_user_timezone_or_default, get_notification_settings
from moviebot.bot.handlers.seasons import get_series_airing_status
from moviebot.utils.helpers import has_notifications_access

logger = logging.getLogger(__name__)

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
    try:
        from moviebot.database.db_operations import print_daily_stats
        print_daily_stats()
    except Exception as e:
        logger.warning(f"[HOURLY STATS] Ошибка вывода статистики: {e}", exc_info=True)



# Функции для уведомлений о планах (определяем до использования в scheduler)
def send_plan_notification(chat_id, film_id, title, link, plan_type, plan_id=None, user_id=None):
    """Отправляет уведомление о запланированном просмотре"""
    # Используем локальные соединение и курсор
    from moviebot.database.db_connection import get_db_connection, get_db_cursor
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
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
                cursor_local.execute('SELECT is_series FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                movie_row = cursor_local.fetchone()
                if movie_row:
                    is_series = bool(movie_row.get('is_series') if isinstance(movie_row, dict) else movie_row[0])
                   
                    if is_series:
                        cursor_local.execute('''
                            SELECT season_number, episode_number
                            FROM series_tracking
                            WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                            ORDER BY season_number DESC, episode_number DESC
                            LIMIT 1
                        ''', (chat_id, film_id, user_id))
                        last_episode_row = cursor_local.fetchone()
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
                cursor_local.execute('''
                    SELECT streaming_service, streaming_url, streaming_done, ticket_file_id
                    FROM plans
                    WHERE id = %s AND chat_id = %s
                ''', (plan_id, chat_id))
                plan_row = cursor_local.fetchone()
               
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
                        # Показываем выбранный кинотеатр с кнопкой для перехода
                        text += f"\n\n📺 <b>Выбранный онлайн-кинотеатр:</b> {streaming_service}"
                        if not markup:
                            markup = InlineKeyboardMarkup(row_width=1)
                        markup.add(InlineKeyboardButton(streaming_service, url=streaming_url))
                        
                        # Добавляем кнопку "Перейти к описанию", если есть kp_id
                        with db_lock:
                            cursor_local.execute('SELECT kp_id FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                            movie_row = cursor_local.fetchone()
                            kp_id = None
                            if movie_row:
                                kp_id = movie_row.get('kp_id') if isinstance(movie_row, dict) else movie_row[0]
                        
                        if kp_id:
                            try:
                                kp_id_int = int(kp_id)
                                markup.add(InlineKeyboardButton("◀️ Перейти к описанию", callback_data=f"back_to_film:{kp_id_int}"))
                            except:
                                pass
                        
                        logger.info(f"[PLAN NOTIFICATION] Показываем выбранный кинотеатр {streaming_service} для плана {plan_id}")
                    else:
                        # Если кинотеатр не выбран, не показываем кнопки (пользователь может выбрать позже через сообщение планирования)
                        logger.info(f"[PLAN NOTIFICATION] Кинотеатр не выбран для плана {plan_id}")
       
        # Новый блок для планов "в кино"
        elif plan_type == 'cinema' and plan_id:
            with db_lock:
                cursor_local.execute('SELECT ticket_file_id FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
                row = cursor_local.fetchone()
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
       
        # Импортируем plan_notification_messages из states
        try:
            from moviebot.states import plan_notification_messages
            plan_notification_messages[msg.message_id] = {
                'link': link,
                'film_id': film_id,
                'plan_id': plan_id
            }
        except Exception as import_e:
            logger.warning(f"[PLAN NOTIFICATION] Не удалось импортировать plan_notification_messages: {import_e}")
       
        logger.info(f"[PLAN NOTIFICATION] Уведомление отправлено для фильма {title} в чат {chat_id}, message_id={msg.message_id}, plan_id={plan_id}")
       
        # КРИТИЧЕСКИ ВАЖНО: Обновляем флаг notification_sent СРАЗУ после отправки, используя существующее соединение
        if plan_id:
            try:
                with db_lock:
                    cursor_local.execute('UPDATE plans SET notification_sent = TRUE WHERE id = %s', (plan_id,))
                    conn_local.commit()
                logger.info(f"[PLAN NOTIFICATION] План {plan_id} отмечен как уведомление отправлено")
            except Exception as e:
                logger.error(f"[PLAN NOTIFICATION] Не удалось отметить план {plan_id} как отправленный: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"[PLAN NOTIFICATION] Ошибка отправки уведомления: {e}", exc_info=True)
    finally:
        # Закрываем локальные соединения
        if 'cursor_local' in locals():
            try:
                cursor_local.close()
            except:
                pass
        if 'conn_local' in locals():
            try:
                conn_local.close()
            except:
                pass

def send_ticket_notification(chat_id, plan_id):
    """Отправляет напоминание с билетами за 10 минут до сеанса"""
    from moviebot.database.db_connection import get_db_connection, get_db_cursor
    
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    try:
        with db_lock:
            cursor_local.execute('''
                SELECT p.ticket_file_id, COALESCE(p.custom_title, m.title, 'Мероприятие') as title, p.plan_datetime
                FROM plans p
                LEFT JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.id = %s AND p.chat_id = %s
            ''', (plan_id, chat_id))
            ticket_row = cursor_local.fetchone()

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
                cursor_local.execute('''
                    UPDATE plans 
                    SET ticket_notification_sent = TRUE 
                    WHERE id = %s
                ''', (plan_id,))
                conn_local.commit()
            logger.info(f"[TICKET NOTIFICATION] План {plan_id} отмечен как уведомление с билетами отправлено")
        except Exception as e:
            logger.warning(f"[TICKET NOTIFICATION] Не удалось отметить план {plan_id} как отправленный: {e}")

        logger.info(f"[TICKET NOTIFICATION] Напоминание с билетами отправлено для {title} в чат {chat_id}")
    except Exception as e:
        logger.error(f"[TICKET NOTIFICATION] Ошибка отправки напоминания: {e}", exc_info=True)
    finally:
        # Закрываем локальные соединения
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass


def check_and_send_plan_notifications():
    """Периодическая проверка планов и отправка пропущенных уведомлений"""

    try:

        now_utc = datetime.now(pytz.utc)

        # Проверяем планы на ближайшие сутки и пропущенные за последние 30 минут

        check_start = now_utc - timedelta(minutes=30)

        check_end = now_utc + timedelta(days=1)

        

        # Используем локальное соединение вместо глобального для избежания проблем с закрытыми соединениями
        from moviebot.database.db_connection import get_db_connection, get_db_cursor
        
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        
        # КРИТИЧЕСКИЙ ФИКС: Добавляем rollback при ошибках транзакции
        try:
            # Сначала делаем rollback на случай если предыдущая транзакция упала
            try:
                conn_local.rollback()
            except:
                pass
        except:
            pass
        
        plans = []
        try:
            with db_lock:
                try:
                    # Проверяем, что курсор не закрыт
                    if cursor_local.closed:
                        logger.warning("[PLAN CHECK] Курсор закрыт, создаем новый")
                        cursor_local.close()
                        cursor_local = get_db_cursor()
                    
                    cursor_local.execute('''

                        SELECT p.id, p.chat_id, p.film_id, p.plan_type, p.plan_datetime, p.user_id,

                               COALESCE(p.custom_title, m.title, 'Мероприятие') as title, m.link, p.notification_sent, p.ticket_notification_sent, p.ticket_file_id

                        FROM plans p

                        LEFT JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id

                        WHERE p.plan_datetime >= %s 

                          AND p.plan_datetime <= %s

                    ''', (check_start, check_end))

                    plans = cursor_local.fetchall()
                except Exception as db_e:
                    logger.error(f"[PLAN CHECK] Ошибка при запросе планов: {db_e}", exc_info=True)
                    try:
                        conn_local.rollback()
                    except:
                        pass
                    plans = []
        except Exception as lock_e:
            logger.error(f"[PLAN CHECK] Ошибка при блокировке БД: {lock_e}", exc_info=True)
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

                ticket_file_id = plan.get('ticket_file_id')

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

                ticket_file_id = plan[10] if len(plan) > 10 else None

            

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
                if reminder_utc:
                    reminder_time_diff = (reminder_utc - now_utc).total_seconds()
                    
                    if reminder_time_diff > 5 and not notification_sent:
                        # Напоминание в будущем (минимум 5 секунд) - планируем уведомление
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
                            # Не логируем, если job уже существует - это нормально при периодических проверках
                        except Exception as e:
                            logger.warning(f"[PLAN CHECK] Не удалось запланировать напоминание для плана {plan_id}: {e}")

                    elif reminder_time_diff <= 5 and reminder_utc >= now_utc - timedelta(minutes=30):
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
                                # Не логируем, если job уже существует - это нормально при периодических проверках
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

                

                # ticket_file_id уже получен из основного запроса выше

                

                if ticket_file_id and ticket_utc:

                    # Планируем напоминание с билетами, если оно еще не запланировано и время еще не прошло
                    # Проверяем, не было ли уже отправлено уведомление с билетами
                    ticket_time_diff = (ticket_utc - now_utc).total_seconds()
                    
                    if ticket_time_diff > 5 and not ticket_notification_sent:

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

                            # Не логируем, если job уже существует - это нормально при периодических проверках

                        except Exception as e:

                            logger.warning(f"[PLAN CHECK] Не удалось запланировать уведомление с билетами для плана {plan_id}: {e}")

                    elif ticket_time_diff <= 5 and ticket_utc >= now_utc - timedelta(minutes=30):

                        # Время напоминания с билетами уже прошло, но не более 30 минут назад - отправляем сразу
                        # Проверяем, не было ли уже отправлено уведомление с билетами
                        if not ticket_notification_sent:
                            try:
                                # Проверяем, не запланировано ли уже уведомление
                                job_id = f'ticket_notify_{chat_id}_{plan_id}_{int(ticket_utc.timestamp())}'
                                existing_job = scheduler.get_job(job_id)
                                if not existing_job:
                                    # Отправляем уведомление ВНЕ блокировки
                                    send_ticket_notification(chat_id, plan_id)
                                    # Отмечаем как отправленное (send_ticket_notification уже обновляет БД, но на всякий случай)
                                    try:
                                        with db_lock:
                                            cursor_local.execute('UPDATE plans SET ticket_notification_sent = TRUE WHERE id = %s', (plan_id,))
                                            conn_local.commit()
                                    except Exception as update_e:
                                        logger.warning(f"[PLAN CHECK] Не удалось обновить ticket_notification_sent для плана {plan_id}: {update_e}")
                                    logger.info(f"[PLAN CHECK] Уведомление с билетами отправлено сразу для плана {plan_id} (фильм {title})")
                                # Не логируем, если job уже существует - это нормально при периодических проверках
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
                
                # Проверяем, прошло ли время плана (с запасом в 5 секунд для надежности)
                time_diff = (plan_utc - now_utc).total_seconds()
                
                if time_diff > 5 and not notification_sent:
                    # План в будущем (минимум 5 секунд) - планируем уведомление на время плана
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
                        # Не логируем, если job уже существует - это нормально при периодических проверках
                    except Exception as e:
                        logger.warning(f"[PLAN CHECK] Не удалось запланировать уведомление на время плана {plan_id}: {e}")
                        
                elif time_diff <= 5 and plan_utc >= now_utc - timedelta(minutes=30):
                    # Время плана уже прошло, но не более 30 минут назад - отправляем сразу
                    # КРИТИЧЕСКИ ВАЖНО: Перечитываем флаг из БД перед проверкой, чтобы избежать дублирования
                    notification_sent_current = notification_sent
                    try:
                        with db_lock:
                            # Используем существующее соединение для проверки
                            cursor_local.execute('SELECT notification_sent FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
                            sent_row = cursor_local.fetchone()
                            if sent_row:
                                notification_sent_current = bool(sent_row.get('notification_sent') if isinstance(sent_row, dict) else sent_row[0])
                    except Exception as read_e:
                        logger.warning(f"[PLAN CHECK] Не удалось перечитать notification_sent для плана {plan_id}: {read_e}")
                    
                    if not notification_sent_current:
                        try:
                            job_id = f'plan_notify_home_{chat_id}_{plan_id}_{int(plan_utc.timestamp())}'
                            existing_job = scheduler.get_job(job_id)
                            if not existing_job:
                                # Перед отправкой еще раз проверяем флаг в БД с блокировкой
                                try:
                                    with db_lock:
                                        cursor_local.execute('SELECT notification_sent FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
                                        final_check = cursor_local.fetchone()
                                        if final_check:
                                            is_sent = bool(final_check.get('notification_sent') if isinstance(final_check, dict) else final_check[0])
                                            if is_sent:
                                                logger.info(f"[PLAN CHECK] Уведомление уже было отправлено для плана дома {plan_id} (дубликат предотвращен)")
                                                # Пропускаем отправку этого плана, переходим к следующему в цикле
                                                continue
                                    
                                    # Отправляем уведомление ВНЕ блокировки, чтобы избежать дедлоков
                                    send_plan_notification(chat_id, film_id, title, link, plan_type, plan_id=plan_id, user_id=user_id)
                                    logger.info(f"[PLAN CHECK] Уведомление отправлено сразу для плана дома {plan_id} (фильм {title}) на время плана {plan_utc}")
                                except Exception as final_e:
                                    logger.error(f"[PLAN CHECK] Ошибка при финальной проверке для плана {plan_id}: {final_e}", exc_info=True)
                            # Не логируем, если job уже существует - это нормально при периодических проверках
                        except Exception as e:
                            logger.error(f"[PLAN CHECK] Ошибка отправки уведомления для плана {plan_id}: {e}", exc_info=True)
                    else:
                        logger.info(f"[PLAN CHECK] Уведомление уже отправлено для плана дома {plan_id}, пропускаем")

    except Exception as e:
        logger.error(f"[PLAN CHECK] Ошибка при проверке планов: {e}", exc_info=True)
    finally:
        # Закрываем локальные соединения
        if 'cursor_local' in locals():
            try:
                cursor_local.close()
            except:
                pass
        if 'conn_local' in locals():
            try:
                conn_local.close()
            except:
                pass



# Настройка периодического вывода статистики
# Вызовы scheduler.add_job должны быть в moviebot.py после импорта модуля



# Очистка планов

def clean_home_plans():
    """Ежедневно удаляет планы дома на вчерашний день, если по фильму нет оценок.
    Также удаляет все планы дома на прошедшие выходные (суббота и воскресенье) в понедельник."""
    from moviebot.database.db_connection import get_db_connection, get_db_cursor
    
    now = datetime.now(plans_tz)
    today = now.date()
    yesterday = (now - timedelta(days=1)).date()
    today_weekday = today.weekday()  # 0 = Monday, 6 = Sunday

    deleted_count = 0
    
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()

    try:
        with db_lock:
            # Если сегодня понедельник, удаляем все планы дома на прошедшие выходные (суббота и воскресенье)
            if today_weekday == 0:  # Monday
                # Находим субботу и воскресенье прошлой недели
                saturday = yesterday - timedelta(days=1)  # Вчера было воскресенье, значит суббота - позавчера
                sunday = yesterday

                cursor_local.execute('''
                    SELECT p.id, p.film_id, p.chat_id, m.title, m.link
                    FROM plans p
                    JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                    WHERE p.plan_type = 'home' 
                    AND DATE(p.plan_datetime AT TIME ZONE 'Europe/Moscow') IN (%s, %s)
                ''', (saturday, sunday))

                weekend_rows = cursor_local.fetchall()

                for row in weekend_rows:
                    plan_id = row.get('id') if isinstance(row, dict) else row[0]
                    film_id = row.get('film_id') if isinstance(row, dict) else row[1]
                    chat_id = row.get('chat_id') if isinstance(row, dict) else row[2]
                    title = row.get('title') if isinstance(row, dict) else row[3]
                    link = row.get('link') if isinstance(row, dict) else row[4]
                    
                    cursor_local.execute('DELETE FROM plans WHERE id = %s', (plan_id,))
                    deleted_count += 1
                    
                    if bot:
                        try:
                            message_text = f"📅 План на фильм <b>{title}</b> удалён (выходные прошли)."
                            if link:
                                message_text += f"\n\n{link}"
                            bot.send_message(chat_id, message_text, parse_mode='HTML')
                        except:
                            pass
                
                logger.info(f"Очищены планы дома на выходные: {len(weekend_rows)} планов")
            
            # Находим планы дома на вчера (используем AT TIME ZONE для корректной работы с TIMESTAMP WITH TIME ZONE)
            cursor_local.execute('''
                SELECT p.id, p.film_id, p.chat_id
                FROM plans p
                WHERE p.plan_type = 'home' AND DATE(p.plan_datetime AT TIME ZONE 'Europe/Moscow') = %s
            ''', (yesterday,))

            rows = cursor_local.fetchall()

            for row in rows:
                # RealDictCursor возвращает словари, но поддерживает доступ по индексу
                plan_id = row.get('id') if isinstance(row, dict) else row[0]
                film_id = row.get('film_id') if isinstance(row, dict) else row[1]
                chat_id = row.get('chat_id') if isinstance(row, dict) else row[2]

                # Проверяем, есть ли оценки по фильму
                cursor_local.execute('SELECT COUNT(*) FROM ratings WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))

                count_row = cursor_local.fetchone()

                count = count_row.get('count') if isinstance(count_row, dict) else (count_row[0] if count_row else 0)

                if count == 0:
                    cursor_local.execute('DELETE FROM plans WHERE id = %s', (plan_id,))
                    deleted_count += 1

                    if bot:
                        try:
                            bot.send_message(chat_id, f"📅 План на фильм удалён (нет оценок за вчера).")
                        except:
                            pass

            conn_local.commit()

    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass

    logger.info(f"Очищены планы дома без оценок: {deleted_count} планов")



def clean_cinema_plans():
    """Каждый понедельник удаляет все планы кино (фильмы) и планы мероприятий, которые прошли более 1 дня назад"""
    from moviebot.database.db_connection import get_db_connection, get_db_cursor
    from datetime import datetime, timedelta
    import pytz
    
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    try:
        with db_lock:
            now_utc = datetime.now(pytz.utc)
            yesterday_utc = now_utc - timedelta(days=1)
            
            # Удаляем все планы кино (фильмы) - как было раньше
            cursor_local.execute("DELETE FROM plans WHERE plan_type = 'cinema' AND film_id IS NOT NULL")
            deleted_films = cursor_local.rowcount
            
            # Удаляем мероприятия (film_id IS NULL), которые прошли более 1 дня назад
            cursor_local.execute("""
                DELETE FROM plans 
                WHERE plan_type = 'cinema' 
                AND film_id IS NULL 
                AND plan_datetime < %s
            """, (yesterday_utc,))
            deleted_events = cursor_local.rowcount
            
            conn_local.commit()
        logger.info(f"Очищены планы кино (понедельник): {deleted_films} фильмов, {deleted_events} мероприятий")
    except Exception as e:
        logger.error(f"[CLEAN CINEMA PLANS] Ошибка: {e}", exc_info=True)
    finally:
        # Закрываем локальные соединения
        if 'cursor_local' in locals():
            try:
                cursor_local.close()
            except:
                pass
        if 'conn_local' in locals():
            try:
                conn_local.close()
            except:
                pass



# Голосование для фильмов "в кино" - УДАЛЕНО



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

        # Получаем sources из API
        sources = None
        try:
            sources = get_external_sources(kp_id)
        except Exception as e:
            logger.warning(f"[SERIES NOTIFICATION] Ошибка получения sources для kp_id={kp_id}: {e}")

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
                    conn_tz = get_db_connection()
                    cursor_tz = get_db_cursor()
                    try:
                        with db_lock:
                            cursor_tz.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'timezone'", (chat_id,))
                            tz_row = cursor_tz.fetchone()
                            if tz_row:
                                tz_str = tz_row.get('value') if isinstance(tz_row, dict) else tz_row[0]
                                user_tz = pytz.timezone(tz_str)
                    finally:
                        try:
                            cursor_tz.close()
                        except:
                            pass
                        try:
                            conn_tz.close()
                        except:
                            pass
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
        conn_sub = get_db_connection()
        cursor_sub = get_db_cursor()
        try:
            with db_lock:
                cursor_sub.execute('SELECT subscribed FROM series_subscriptions WHERE chat_id = %s AND film_id = %s AND user_id = %s', (chat_id, film_id, user_id))
                sub_row = cursor_sub.fetchone()
                is_subscribed = sub_row and (sub_row.get('subscribed') if isinstance(sub_row, dict) else sub_row[0])
        finally:
            try:
                cursor_sub.close()
            except:
                pass
            try:
                conn_sub.close()
            except:
                pass
        
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
                conn_tz = get_db_connection()
                cursor_tz = get_db_cursor()
                try:
                    with db_lock:
                        cursor_tz.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'timezone'", (chat_id,))
                        tz_row = cursor_tz.fetchone()
                        if tz_row:
                            tz_str = tz_row.get('value') if isinstance(tz_row, dict) else tz_row[0]
                            user_tz = pytz.timezone(tz_str)
                finally:
                    try:
                        cursor_tz.close()
                    except:
                        pass
                    try:
                        conn_tz.close()
                    except:
                        pass
            except:
                pass
            
            # Уведомление за день до выхода
            notification_time = next_episode_date - timedelta(days=1)
            notification_time = user_tz.localize(notification_time.replace(hour=10, minute=0))
            
            with db_lock:
                conn_title = get_db_connection()
                cursor_title = get_db_cursor()
                try:
                    cursor_title.execute("SELECT title FROM movies WHERE id = %s", (film_id,))
                    title_row = cursor_title.fetchone()
                    title = title_row.get('title') if title_row and isinstance(title_row, dict) else (title_row[0] if title_row else "Сериал")
                finally:
                    try:
                        cursor_title.close()
                    except:
                        pass
                    try:
                        conn_title.close()
                    except:
                        pass
            
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
    from moviebot.database.db_connection import get_db_connection, get_db_cursor
    
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        # Проверяем, не оценил ли уже пользователь
        with db_lock:
            cursor_local.execute("""
                SELECT id FROM ratings 
                WHERE chat_id = %s AND film_id = %s AND user_id = %s
            """, (chat_id, film_id, user_id))
            has_rating = cursor_local.fetchone()
            
            if has_rating:
                logger.info(f"[RATING REMINDER] Пользователь {user_id} уже оценил фильм {film_id}, пропускаем")
                return
            
            # Получаем ссылку на фильм
            cursor_local.execute("SELECT link FROM movies WHERE id = %s", (film_id,))
            film_row = cursor_local.fetchone()
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
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass


def check_subscription_payments():
    """Проверяет подписки и отправляет уведомления за день до списания"""
    from moviebot.database.db_connection import get_db_connection, get_db_cursor
    
    if not bot:
        return
    
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        from moviebot.database.db_operations import get_active_subscription        
        now = datetime.now(pytz.UTC)
        tomorrow = now + timedelta(days=1)
        
        # Находим подписки, у которых next_payment_date завтра
        with db_lock:
            cursor_local.execute("""
                SELECT id, chat_id, user_id, subscription_type, plan_type, period_type, price, next_payment_date
                FROM subscriptions
                WHERE is_active = TRUE
                AND next_payment_date IS NOT NULL
                AND DATE(next_payment_date AT TIME ZONE 'UTC') = DATE(%s AT TIME ZONE 'UTC')
            """, (tomorrow,))
            subscriptions = cursor_local.fetchall()
        
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
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass


def send_successful_payment_notification(
    chat_id: int,
    subscription_id: int,
    subscription_type: str,
    plan_type: str,
    period_type: str,
    is_recurring: bool = False,
    check_url: Optional[str] = None,
    pdf_url: Optional[str] = None
):
    """Отправляет уведомление об успешном платеже с чеком от самозанятого (если есть)"""
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
            # Уведомление для первичных платежей
            if subscription_type == 'group':
                # Для групповых подписок - полное сообщение со списком функций
                plan_names_full = {
                    'notifications': 'Уведомления о сериалах',
                    'recommendations': 'Рекомендации',
                    'tickets': 'Билеты',
                    'all': 'Все режимы'
                }
                tariff_name = plan_names_full.get(plan_type, plan_type)
                
                text = "Спасибо за покупку! 🎉\n\n"
                text += f"Ваша новая подписка: <b>{tariff_name}</b>\n\n"
                text += "Вот какой функционал вам теперь доступен:\n\n"
                
                # Формируем описание функций
                if plan_type == 'all':
                    text += "📦 <b>Все режимы:</b>\n\n"
                    text += "🔔 <b>Уведомления о сериалах:</b>\n"
                    text += "• Автоматические уведомления о выходе новых серий\n"
                    text += "• Настройка времени уведомлений (будни/выходные)\n"
                    text += "• Персонализированные напоминания для каждого сериала\n"
                    text += "• Отслеживание прогресса просмотра сезонов\n\n"
                    text += "🎯 <b>Персональные рекомендации:</b>\n"
                    text += "• Режим \"По оценкам в базе\" — рекомендации по оценкам фильмов, добавленных в базу чата или группы\n"
                    text += "• Режим \"Рандом по Кинопоиску\" — случайный фильм из Кинопоиска по фильтрам\n"
                    text += "• Режим рандомайзера \"По моим оценкам\" — рекомендации по оценкам из Кинопоиска\n"
                    text += "• Импорт базы из Кинопоиска\n\n"
                    text += "🎫 <b>Билеты в кино:</b>\n"
                    text += "• Добавление билетов на сеансы и мероприятия\n"
                    text += "• Настраиваемые уведомления с билетами перед мероприятием\n"
                elif plan_type == 'notifications':
                    text += "🔔 <b>Уведомления о сериалах:</b>\n"
                    text += "• Автоматические уведомления о выходе новых серий\n"
                    text += "• Настройка времени уведомлений (будни/выходные)\n"
                    text += "• Персонализированные напоминания для каждого сериала\n"
                    text += "• Отслеживание прогресса просмотра сезонов\n"
                elif plan_type == 'recommendations':
                    text += "🎯 <b>Персональные рекомендации:</b>\n"
                    text += "• Режим \"По оценкам в базе\" — рекомендации по оценкам фильмов, добавленных в базу чата или группы\n"
                    text += "• Режим \"Рандом по Кинопоиску\" — случайный фильм из Кинопоиска по фильтрам\n"
                    text += "• Режим рандомайзера \"По моим оценкам\" — рекомендации по оценкам из Кинопоиска\n"
                    text += "• Импорт базы из Кинопоиска\n"
                elif plan_type == 'tickets':
                    text += "🎫 <b>Билеты в кино:</b>\n"
                    text += "• Добавление билетов на сеансы и мероприятия\n"
                    text += "• Настраиваемые уведомления с билетами перед мероприятием\n"
            else:
                # Для личных подписок - краткое сообщение
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
        
        # === ДОБАВЛЯЕМ ЧЕК ОТ САМОЗАНЯТОГО ===
        if check_url:
            text += "\n\n📄 <b>Чек от самозанятого:</b>\n"
            text += f"{check_url}\n"
            if pdf_url:
                text += f"\n📥 <a href=\"{pdf_url}\">Скачать чек в PDF</a>"
        
        markup = InlineKeyboardMarkup()
        
        # Для групповых подписок показываем список участников и возможность добавления других
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
                if BOT_ID and BOT_ID in active_users:
                    active_users = {uid: uname for uid, uname in active_users.items() if uid != BOT_ID}
                
                group_size = sub.get('group_size')
                members_count = len(members) if members else 0
                available_slots = (group_size - members_count) if group_size else 0
                
                # Добавляем информацию об участниках подписки
                text += "\n\n"
                text += "👥 <b>Участники подписки:</b>\n"
                if members:
                    for user_id_member, username_member in list(members.items())[:10]:
                        display_name = username_member if username_member.startswith('user_') else f"@{username_member}"
                        text += f"• {display_name}\n"
                    if len(members) > 10:
                        text += f"... и еще {len(members) - 10} участник(ов)\n"
                else:
                    text += "Пока нет участников\n"
                
                text += f"\n✅ Участников в подписке: <b>{members_count}</b>"
                if group_size:
                    text += f" из <b>{group_size}</b>"
                
                # Находим участников группы, которые не в подписке
                not_in_subscription = []
                for user_id_member, username_member in active_users.items():
                    if user_id_member not in members:
                        not_in_subscription.append({
                            'user_id': user_id_member,
                            'username': username_member
                        })
                
                # Если есть доступные места и участники для добавления, предлагаем их добавить
                if available_slots > 0 and not_in_subscription:
                    text += "\n\n"
                    text += f"➕ <b>Доступно мест: {available_slots}</b>\n"
                    text += "Добавьте участников группы в подписку:\n\n"
                    
                    # Добавляем кнопки для добавления участников (максимум доступных мест или 10, что меньше)
                    max_buttons = min(available_slots, 10, len(not_in_subscription))
                    for member in not_in_subscription[:max_buttons]:
                        display_name = member['username'] if member['username'].startswith('user_') else f"@{member['username']}"
                        button_text = f"➕ {display_name}"
                        if len(button_text) > 50:
                            button_text = button_text[:47] + "..."
                        markup.add(InlineKeyboardButton(button_text, callback_data=f"payment:add_member:{subscription_id}:{member['user_id']}"))
                    
                    # Если участников больше, чем доступных мест, показываем кнопку для выбора участников
                    if len(not_in_subscription) > max_buttons or available_slots > max_buttons:
                        markup.add(InlineKeyboardButton("👥 Выбрать участников", callback_data=f"payment:select_members:{subscription_id}"))
                elif available_slots == 0 and group_size:
                    text += "\n\n"
                    text += "⚠️ Все места заняты. Для добавления новых участников расширьте подписку."
            except Exception as e:
                logger.error(f"[SUCCESSFUL PAYMENT] Ошибка при получении участников для добавления: {e}", exc_info=True)
        
        markup.add(InlineKeyboardButton("✅ Готово", callback_data="payment:success_ok"))
        markup.add(InlineKeyboardButton("◀️ Главное меню", callback_data="back_to_start_menu"))
        
        # Для личных подписок отправляем в личку пользователя, для групповых - в групповой чат
        if subscription_type == 'personal':
            target_chat_id = sub.get('user_id')
        else:
            # Для групповых подписок отправляем в группу
            target_chat_id = chat_id
        
        try:
            bot.send_message(target_chat_id, text, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True)
            user_id_log = sub.get('user_id', 'N/A')
            chat_id_log = sub.get('chat_id', 'N/A')
            logger.info(f"[SUCCESSFUL PAYMENT] Уведомление отправлено: subscription_id={subscription_id}, user_id={user_id_log}, chat_id={chat_id_log}, subscription_type={subscription_type}, plan_type={plan_type}, period_type={period_type} (check={'ДА' if check_url else 'НЕТ'})")
        except Exception as e:
            logger.error(f"[SUCCESSFUL PAYMENT] Ошибка отправки уведомления: {e}")
        
        # === ОТПРАВКА СООБЩЕНИЙ АДМИНАМ И СОЗДАТЕЛЮ ===
        try:
            from moviebot.utils.admin import get_all_admins, is_owner
            from moviebot.states import user_check_receipt_state
            from moviebot.bot.handlers.admin import OWNER_ID
            from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock  # Локальный импорт (как в других местах scheduler.py)
            
            # Получаем информацию о подписке для админов
            sub_user_id = sub.get('user_id')
            sub_chat_id = sub.get('chat_id')
            sub_price = sub.get('price', 0)  # Полная цена подписки (fallback)
            
            # Получаем реальную сумму последнего платежа (для upgrade — доплата)
            actual_amount = sub_price
            from moviebot.database.db_connection import get_db_connection, get_db_cursor
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                with db_lock:
                    cursor_local.execute("""
                        SELECT amount FROM payments 
                        WHERE subscription_id = %s 
                        ORDER BY created_at DESC 
                        LIMIT 1
                    """, (subscription_id,))
                    row = cursor_local.fetchone()
                    if row:
                        actual_amount = float(row['amount'])
            except Exception as e:
                logger.error(f"[SUCCESSFUL PAYMENT] Не удалось получить сумму платежа из БД: {e}")
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            # Определяем ID получателя
            target_id = sub_chat_id if subscription_type == 'group' else sub_user_id
            
            # Получаем название чата или пользователя
            target_name = None
            try:
                if subscription_type == 'group':
                    chat_info = bot.get_chat(target_id)
                    target_name = chat_info.title if hasattr(chat_info, 'title') else f"Группа {target_id}"
                else:
                    user_info = bot.get_chat(target_id)
                    target_name = user_info.first_name if hasattr(user_info, 'first_name') else f"Пользователь {target_id}"
            except Exception as e:
                logger.error(f"[SUCCESSFUL PAYMENT] Ошибка получения информации о чате/пользователе: {e}")
                target_name = f"ID: {target_id}"
            
            # Формируем сообщение для админов
            admin_text = "Привет!\n"
            admin_text += f"Оформлен платеж на: <b>{plan_name}</b>\n"
            if subscription_type == 'group':
                admin_text += f"<b>ID чата группы: {target_id}</b>\n"
            else:
                admin_text += f"<b>ID пользователя: {target_id}</b>\n"
            
            if actual_amount < sub_price:
                admin_text += f"Доплата за upgrade: <b>{actual_amount:.2f}₽</b>\n"
                admin_text += f"Новая полная сумма подписки: <b>{sub_price}₽</b>\n"
            else:
                admin_text += f"Сумма: <b>{actual_amount:.2f}₽</b>\n"
            
            admin_text += "\nОтправьте чек в ответ на это сообщение."
            
            # Получаем всех админов
            admins = get_all_admins()
            admin_ids = [admin['user_id'] for admin in admins]
            
            # Добавляем создателя, если его нет в списке админов
            if OWNER_ID not in admin_ids:
                admin_ids.append(OWNER_ID)
            
            # Отправляем сообщение каждому админу
            for admin_id in admin_ids:
                try:
                    sent_msg = bot.send_message(admin_id, admin_text, parse_mode='HTML')
                    
                    # Сохраняем информацию о сообщении для обработки реплая
                    user_check_receipt_state[sent_msg.message_id] = {
                        'target_chat_id': target_id,
                        'subscription_id': subscription_id,
                        'subscription_type': subscription_type,
                        'target_name': target_name
                    }
                    
                    logger.info(f"[SUCCESSFUL PAYMENT] Сообщение админу отправлено: admin_id={admin_id}, message_id={sent_msg.message_id}, target_id={target_id}")
                except Exception as e:
                    logger.error(f"[SUCCESSFUL PAYMENT] Ошибка отправки сообщения админу {admin_id}: {e}")
        except Exception as e:
            logger.error(f"[SUCCESSFUL PAYMENT] Ошибка отправки сообщений админам: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[SUCCESSFUL PAYMENT] Ошибка: {e}", exc_info=True)


def process_recurring_payments():
    """Выполняет безакцептные списания для подписок с payment_method_id"""
    
    if not bot:  # bot должен быть глобальным или импортированным
        return
    
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from moviebot.config import DATABASE_URL
    from moviebot.database.db_connection import db_lock
    import logging
    from datetime import datetime, timedelta
    import pytz
    import uuid
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    
    logger = logging.getLogger(__name__)
    
    from moviebot.api.yookassa_api import create_recurring_payment
    from moviebot.database.db_operations import renew_subscription, save_payment, update_payment_status
    from moviebot.services.nalog_service import create_check
    
    now = datetime.now(pytz.UTC)
    
    subscriptions = []
    
    # Основной SELECT подписок — короткий lock только на fetch
    conn_main = None
    cursor_main = None
    try:
        conn_main = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor_main = conn_main.cursor()
        
        with db_lock:  # Коротко: только execute + fetch
            cursor_main.execute("""
                SELECT id, chat_id, user_id, subscription_type, plan_type, period_type, price, 
                       next_payment_date, payment_method_id, telegram_username, group_username, group_size
                FROM subscriptions
                WHERE is_active = TRUE
                AND next_payment_date IS NOT NULL
                AND payment_method_id IS NOT NULL
                AND period_type != 'lifetime'
                AND DATE(next_payment_date AT TIME ZONE 'UTC') <= DATE(%s AT TIME ZONE 'UTC')
            """, (now,))
            subscriptions = cursor_main.fetchall()
            
    except Exception as db_e:
        logger.error(f"[RECURRING PAYMENT] Ошибка при запросе подписок: {db_e}", exc_info=True)
        subscriptions = []
    finally:
        if cursor_main:
            try:
                cursor_main.close()
            except:
                pass
        if conn_main:
            try:
                conn_main.close()
            except:
                pass
    
    # Обработка каждой подписки (всё вне lock — API, уведомления и т.д.)
    for sub in subscriptions:
        try:
            subscription_id = sub['id']
            chat_id = sub['chat_id']
            user_id = sub['user_id']
            subscription_type = sub['subscription_type']
            plan_type = sub['plan_type']
            period_type = sub['period_type']
            price = float(sub['price'])
            payment_method_id = sub['payment_method_id']
            telegram_username = sub['telegram_username']
            group_username = sub['group_username']
            group_size = sub['group_size']
            
            # Проверяем, был ли первый платеж с промокодом только на первый месяц
            # Если да, используем полную стоимость тарифа для рекуррентных платежей
            recurring_amount = price  # По умолчанию используем цену из подписки
            is_first_month_promo = False
            
            try:
                conn_check = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
                cursor_check = conn_check.cursor()
                try:
                    with db_lock:
                        # Получаем первый платеж для этой подписки (по yookassa_payment_id)
                        cursor_check.execute("""
                            SELECT yookassa_payment_id, amount
                            FROM payments
                            WHERE subscription_id = %s
                            ORDER BY created_at ASC
                            LIMIT 1
                        """, (subscription_id,))
                        first_payment = cursor_check.fetchone()
                        
                        if first_payment and first_payment.get('yookassa_payment_id'):
                            # Получаем metadata из YooKassa для первого платежа
                            yookassa_payment_id = first_payment.get('yookassa_payment_id')
                            try:
                                from moviebot.api.yookassa_api import get_payment_info
                                first_payment_obj = get_payment_info(yookassa_payment_id)
                                
                                if first_payment_obj and hasattr(first_payment_obj, 'metadata') and first_payment_obj.metadata:
                                    payment_metadata = first_payment_obj.metadata
                                    
                                    # Проверяем, был ли промокод только на первый месяц
                                    if payment_metadata.get('is_first_month_promo', 'false').lower() == 'true':
                                        is_first_month_promo = True
                                        original_price_str = payment_metadata.get('original_price')
                                        
                                        if original_price_str:
                                            try:
                                                # Используем полную стоимость из metadata первого платежа
                                                recurring_amount = float(original_price_str)
                                                logger.info(f"[RECURRING PAYMENT] Промокод только на первый месяц обнаружен. Используем полную стоимость: {recurring_amount}₽ вместо {price}₽")
                                            except (ValueError, TypeError):
                                                # Если не удалось распарсить, используем базовую цену БЕЗ скидок
                                                from moviebot.bot.callbacks.payment_callbacks import get_base_price
                                                recurring_amount = get_base_price(
                                                    subscription_type=subscription_type,
                                                    plan_type=plan_type,
                                                    period_type=period_type,
                                                    group_size=group_size
                                                )
                                                logger.info(f"[RECURRING PAYMENT] Используем базовую стоимость (без скидок): {recurring_amount}₽")
                                        else:
                                            # Если original_price нет в metadata, используем базовую цену БЕЗ скидок
                                            from moviebot.bot.callbacks.payment_callbacks import get_base_price
                                            recurring_amount = get_base_price(
                                                subscription_type=subscription_type,
                                                plan_type=plan_type,
                                                period_type=period_type,
                                                group_size=group_size
                                            )
                                            logger.info(f"[RECURRING PAYMENT] Используем базовую стоимость (original_price отсутствует): {recurring_amount}₽")
                            except Exception as yookassa_error:
                                logger.warning(f"[RECURRING PAYMENT] Не удалось получить metadata из YooKassa для первого платежа {yookassa_payment_id}: {yookassa_error}")
                                # Если не удалось получить metadata из YooKassa, используем базовую цену БЕЗ скидок
                                # (на случай, если был промокод, но metadata недоступен)
                                from moviebot.bot.callbacks.payment_callbacks import get_base_price
                                recurring_amount = get_base_price(
                                    subscription_type=subscription_type,
                                    plan_type=plan_type,
                                    period_type=period_type,
                                    group_size=group_size
                                )
                                logger.info(f"[RECURRING PAYMENT] Используем базовую стоимость (metadata недоступен): {recurring_amount}₽")
                finally:
                    try:
                        cursor_check.close()
                    except:
                        pass
                    try:
                        conn_check.close()
                    except:
                        pass
            except Exception as check_error:
                logger.error(f"[RECURRING PAYMENT] Ошибка проверки промокода: {check_error}", exc_info=True)
                # В случае ошибки используем цену из подписки
            
            logger.info(f"[RECURRING PAYMENT] Обработка подписки {subscription_id}, payment_method_id={payment_method_id}, сумма={recurring_amount}₽ (is_first_month_promo={is_first_month_promo})")
            
            payment = create_recurring_payment(
                user_id=user_id,
                chat_id=chat_id,
                subscription_type=subscription_type,
                plan_type=plan_type,
                period_type=period_type,
                amount=recurring_amount,  # Используем полную стоимость для рекуррентных платежей
                payment_method_id=payment_method_id,
                group_size=group_size,
                telegram_username=telegram_username,
                group_username=group_username
            )
            
            if not payment:
                logger.error(f"[RECURRING PAYMENT] Не удалось создать платеж для подписки {subscription_id}")
                continue
            
            payment_id = payment.metadata.get('payment_id') if hasattr(payment, 'metadata') and payment.metadata else str(uuid.uuid4())
            
            logger.info(f"[RECURRING PAYMENT] Платеж создан: {payment.id}, статус: {payment.status}")
            
            save_payment(
                payment_id=payment_id,
                yookassa_payment_id=payment.id,
                user_id=user_id,
                chat_id=chat_id,
                subscription_type=subscription_type,
                plan_type=plan_type,
                period_type=period_type,
                group_size=group_size,
                amount=recurring_amount,  # Сохраняем полную стоимость для рекуррентных платежей
                status=payment.status
            )
            
            if payment.status == 'succeeded':
                # Проверяем, есть ли будущая подписка с activated_at = next_payment_date
                # Если есть, отменяем текущую и активируем будущую
                conn_future = None
                cursor_future = None
                future_subscription_id = None
                try:
                    conn_future = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
                    cursor_future = conn_future.cursor()
                    
                    with db_lock:
                        # Ищем будущую подписку с activated_at = next_payment_date для этого пользователя/чата
                        next_payment_date = sub.get('next_payment_date')
                        if next_payment_date:
                            cursor_future.execute("""
                                SELECT id, plan_type, period_type, price
                                FROM subscriptions
                                WHERE user_id = %s AND chat_id = %s 
                                AND subscription_type = %s
                                AND is_active = TRUE
                                AND activated_at = %s
                                AND id != %s
                                LIMIT 1
                            """, (user_id, chat_id, subscription_type, next_payment_date, subscription_id))
                            future_sub = cursor_future.fetchone()
                            
                            if future_sub:
                                future_subscription_id = future_sub['id']
                                future_plan_type = future_sub['plan_type']
                                future_period_type = future_sub['period_type']
                                future_price = float(future_sub['price'])
                                
                                logger.info(f"[RECURRING PAYMENT] Найдена будущая подписка {future_subscription_id} для активации")
                                
                                # Отменяем текущую подписку
                                cursor_future.execute("""
                                    UPDATE subscriptions 
                                    SET is_active = FALSE, cancelled_at = %s
                                    WHERE id = %s
                                """, (now, subscription_id))
                                
                                # Активируем будущую подписку (устанавливаем activated_at = now)
                                from dateutil.relativedelta import relativedelta
                                if future_period_type == 'month':
                                    new_expires_at = now + relativedelta(months=1)
                                    new_next_payment = now + relativedelta(months=1)
                                elif future_period_type == '3months':
                                    new_expires_at = now + relativedelta(months=3)
                                    new_next_payment = now + relativedelta(months=3)
                                elif future_period_type == 'year':
                                    new_expires_at = now + relativedelta(years=1)
                                    new_next_payment = now + relativedelta(years=1)
                                elif future_period_type == 'lifetime':
                                    new_expires_at = None
                                    new_next_payment = None
                                else:
                                    new_expires_at = now + timedelta(days=30)
                                    new_next_payment = now + timedelta(days=30)
                                
                                cursor_future.execute("""
                                    UPDATE subscriptions 
                                    SET activated_at = %s, expires_at = %s, next_payment_date = %s
                                    WHERE id = %s
                                """, (now, new_expires_at, new_next_payment, future_subscription_id))
                                
                                conn_future.commit()
                                
                                logger.info(f"[RECURRING PAYMENT] Отменена старая подписка {subscription_id}, активирована новая {future_subscription_id}")
                                
                                # Используем параметры будущей подписки для уведомления
                                subscription_id = future_subscription_id
                                plan_type = future_plan_type
                                period_type = future_period_type
                                price = future_price
                            else:
                                # Нет будущей подписки - обычное продление
                                renew_subscription(subscription_id, period_type)
                except Exception as future_error:
                    logger.error(f"[RECURRING PAYMENT] Ошибка проверки будущей подписки: {future_error}", exc_info=True)
                    # Fallback на обычное продление
                    renew_subscription(subscription_id, period_type)
                finally:
                    if cursor_future:
                        try:
                            cursor_future.close()
                        except:
                            pass
                    if conn_future:
                        try:
                            conn_future.close()
                        except:
                            pass
                
                update_payment_status(payment_id, 'succeeded', subscription_id)
                
                description = f"Автопродление подписки \"{plan_type}\" на {period_type}"
                user_name = telegram_username or f"user_{user_id}"
                check_url, pdf_url = create_check(amount_rub=price, description=description, user_name=user_name)
                
                send_successful_payment_notification(
                    chat_id=chat_id,
                    subscription_id=subscription_id,
                    subscription_type=subscription_type,
                    plan_type=plan_type,
                    period_type=period_type,
                    is_recurring=True,
                    check_url=check_url,
                    pdf_url=pdf_url
                )
            else:
                # Подсчёт retry_count — отдельный короткий conn + lock
                retry_count = 0
                conn_retry = None
                cursor_retry = None
                try:
                    conn_retry = psycopg2.connect(DATABASE_URL)
                    cursor_retry = conn_retry.cursor()
                    
                    seven_days_ago = now - timedelta(days=7)
                    with db_lock:  # Коротко
                        cursor_retry.execute("""
                            SELECT COUNT(*) 
                            FROM payments 
                            WHERE subscription_id = %s 
                            AND status IN ('canceled', 'pending', 'waiting_for_capture')
                            AND created_at >= %s
                        """, (subscription_id, seven_days_ago))
                        result = cursor_retry.fetchone()
                        retry_count = result[0] if result else 0
                except Exception as e:
                    logger.error(f"[RECURRING PAYMENT] Ошибка подсчета попыток: {e}")
                finally:
                    if cursor_retry:
                        try:
                            cursor_retry.close()
                        except:
                            pass
                    if conn_retry:
                        try:
                            conn_retry.close()
                        except:
                            pass
                
                has_cancellation_details = hasattr(payment, 'cancellation_details') and payment.cancellation_details
                
                if has_cancellation_details and retry_count < 5:
                    tomorrow = now + timedelta(days=1)
                    next_attempt = PLANS_TZ.localize(datetime.combine(tomorrow.date(), datetime.min.time().replace(hour=9, minute=0))).astimezone(pytz.UTC)
                    
                    logger.info(f"[RECURRING PAYMENT] Планируется повторная попытка для подписки {subscription_id} на {next_attempt}")
                    
                    # Update next_payment_date — отдельный короткий conn + lock
                    conn_update = None
                    cursor_update = None
                    try:
                        conn_update = psycopg2.connect(DATABASE_URL)
                        cursor_update = conn_update.cursor()
                        with db_lock:  # Коротко
                            cursor_update.execute("""
                                UPDATE subscriptions 
                                SET next_payment_date = %s
                                WHERE id = %s
                            """, (next_attempt, subscription_id))
                        conn_update.commit()
                    except Exception as e:
                        logger.error(f"[RECURRING PAYMENT] Ошибка обновления next_payment_date: {e}")
                    finally:
                        if cursor_update:
                            try:
                                cursor_update.close()
                            except:
                                pass
                        if conn_update:
                            try:
                                conn_update.close()
                            except:
                                pass
                    
                    text = "🚨 <b>Оплата не прошла!</b>\n\n"
                    if retry_count < 4:
                        text += f"Попытка {retry_count + 1} из 5. Следующая попытка через день."
                    else:
                        text += f"Попытка {retry_count + 1} из 5. Последняя автоматическая попытка."
                    text += "\n\nМожете инициировать платеж вручную ниже."
                    
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("Провести платеж", callback_data=f"payment:retry_payment:{subscription_id}"))
                    markup.add(InlineKeyboardButton("Изменить тариф", callback_data=f"payment:modify:{subscription_id}"))
                    markup.add(InlineKeyboardButton("Отменить подписку", callback_data=f"payment:cancel:{subscription_id}"))
                    
                    target_chat_id = user_id if subscription_type == 'personal' else chat_id
                    try:
                        bot.send_message(target_chat_id, text, reply_markup=markup, parse_mode='HTML')
                    except Exception as e:
                        logger.error(f"[RECURRING PAYMENT] Ошибка отправки уведомления: {e}")
                else:
                    if retry_count >= 5:
                        logger.warning(f"[RECURRING PAYMENT] Превышен лимит попыток для подписки {subscription_id}")
                        
                        # Отключение автоплатежей — отдельный короткий conn + lock
                        conn_disable = None
                        cursor_disable = None
                        try:
                            conn_disable = psycopg2.connect(DATABASE_URL)
                            cursor_disable = conn_disable.cursor()
                            with db_lock:  # Коротко
                                cursor_disable.execute("""
                                    UPDATE subscriptions 
                                    SET payment_method_id = NULL
                                    WHERE id = %s
                                """, (subscription_id,))
                            conn_disable.commit()
                        except Exception as e:
                            logger.error(f"[RECURRING PAYMENT] Ошибка отключения автоплатежей: {e}")
                        finally:
                            if cursor_disable:
                                try:
                                    cursor_disable.close()
                                except:
                                    pass
                            if conn_disable:
                                try:
                                    conn_disable.close()
                                except:
                                    pass
                        
                        text = "⛔ <b>Автоплатежи приостановлены</b>\n\nПосле 5 неудачных попыток автоплатежи отключены. Оплатите вручную."
                        markup = InlineKeyboardMarkup(row_width=1)
                        markup.add(InlineKeyboardButton("Оплатить подписку", callback_data="payment:tariffs"))
                        markup.add(InlineKeyboardButton("Отменить подписку", callback_data=f"payment:cancel:{subscription_id}"))
                        
                        target_chat_id = user_id if subscription_type == 'personal' else chat_id
                        try:
                            bot.send_message(target_chat_id, text, reply_markup=markup, parse_mode='HTML')
                        except Exception as e:
                            logger.error(f"[RECURRING PAYMENT] Ошибка уведомления о приостановке: {e}")
        
        except Exception as e:
            logger.error(f"[RECURRING PAYMENT] Ошибка обработки подписки {subscription_id}: {e}", exc_info=True)


def get_random_events_enabled(chat_id):
    """Проверяет, включены ли случайные события для чата"""
    from moviebot.database.db_connection import get_db_connection, get_db_cursor
    
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    try:
        with db_lock:
            cursor_local.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'random_events_enabled'", (chat_id,))
            row = cursor_local.fetchone()
            if row:
                value = row.get('value') if isinstance(row, dict) else row[0]
                return value == 'true'
        return True  # По умолчанию включено
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass


def was_event_sent_today(chat_id, event_type):
    """Проверяет, было ли отправлено событие/уведомление сегодня для данного чата"""
    from moviebot.database.db_connection import get_db_connection, get_db_cursor
    
    if not bot:
        return False
    
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        now = datetime.now(PLANS_TZ)
        today = now.date()
        with db_lock:
            cursor_local.execute("""
                SELECT id FROM event_notifications 
                WHERE chat_id = %s AND event_type = %s AND sent_date = %s
            """, (chat_id, event_type, today))
            row = cursor_local.fetchone()
            return row is not None
    except Exception as e:
        logger.error(f"[EVENT NOTIFICATIONS] Ошибка проверки события: {e}", exc_info=True)
        return False
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass


def mark_event_sent(chat_id, event_type):
    """Отмечает, что событие/уведомление было отправлено сегодня"""
    from moviebot.database.db_connection import get_db_connection, get_db_cursor
    
    if not bot:
        return
    
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        now = datetime.now(PLANS_TZ)
        today = now.date()
        with db_lock:
            cursor_local.execute("""
                INSERT INTO event_notifications (chat_id, event_type, sent_date)
                VALUES (%s, %s, %s)
                ON CONFLICT (chat_id, event_type, sent_date) DO NOTHING
            """, (chat_id, event_type, today))
            conn_local.commit()
    except Exception as e:
        logger.error(f"[EVENT NOTIFICATIONS] Ошибка сохранения события: {e}", exc_info=True)
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass


def check_weekend_schedule():
    """Проверяет расписание на выходные (пт-сб-вс) и предлагает рандомный фильм, если нет планов домашнего просмотра.
    Выполняется только в пятницу. Если нет планов вообще (ни дома, ни в кино), отправляет уведомление раз в неделю."""
    from moviebot.database.db_connection import get_db_connection, get_db_cursor
    
    if not bot:
        return
    
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        now = datetime.now(PLANS_TZ)
        current_weekday = now.weekday()
        
        # Проверяем только в пятницу (4 = пятница)
        if current_weekday != 4:
            return
        
        # Получаем все групповые чаты
        with db_lock:
            cursor_local.execute("SELECT DISTINCT chat_id FROM movies")
            chat_rows = cursor_local.fetchall()
        
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
            cursor_local.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'reminder_weekend_films_disabled'", (chat_id,))
            reminder_disabled_row = cursor_local.fetchone()
            if reminder_disabled_row:
                is_disabled = reminder_disabled_row.get('value') if isinstance(reminder_disabled_row, dict) else reminder_disabled_row[0]
                if is_disabled == 'true':
                    continue
            
            # Проверяем, есть ли планы на выходные (пт-сб-вс) для домашнего просмотра
            friday = now.replace(hour=0, minute=0, second=0, microsecond=0)
            sunday = now.replace(hour=23, minute=59, second=59, microsecond=0) + timedelta(days=2)
            
            # Проверяем планы домашнего просмотра на выходные
            cursor_local.execute('''
                SELECT COUNT(*) FROM plans
                WHERE chat_id = %s 
                AND plan_type = 'home'
                AND plan_datetime >= %s 
                AND plan_datetime <= %s
            ''', (chat_id, friday, sunday))
            home_plans_count = cursor_local.fetchone()
            home_count = home_plans_count.get('count') if isinstance(home_plans_count, dict) else home_plans_count[0] if home_plans_count else 0
            
            # Проверяем планы в кино на выходные
            cursor_local.execute('''
                SELECT COUNT(*) FROM plans
                WHERE chat_id = %s 
                AND plan_type = 'cinema'
                AND plan_datetime >= %s 
                AND plan_datetime <= %s
            ''', (chat_id, friday, sunday))
            cinema_plans_count = cursor_local.fetchone()
            cinema_count = cinema_plans_count.get('count') if isinstance(cinema_plans_count, dict) else cinema_plans_count[0] if cinema_plans_count else 0
            
            # Если нет планов домашнего просмотра, отправляем уведомление
            if home_count == 0:
                # Проверяем, когда последний раз отправляли уведомление о выходных
                cursor_local.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'last_weekend_reminder_date'", (chat_id,))
                last_date_row = cursor_local.fetchone()
                
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
                        cursor_local.execute('''
                            INSERT INTO settings (chat_id, key, value)
                            VALUES (%s, 'last_weekend_reminder_date', %s)
                            ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                        ''', (chat_id, now.date().isoformat()))
                        conn_local.commit()
                        
                        logger.info(f"[WEEKEND SCHEDULE] Отправлено уведомление о выходных для чата {chat_id}")
                    except Exception as e:
                        logger.error(f"[WEEKEND SCHEDULE] Ошибка при отправке уведомления: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[WEEKEND SCHEDULE] Ошибка в check_weekend_schedule: {e}", exc_info=True)
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass


def check_premiere_reminder():
    """Проверяет, нет ли планов по премьерам, и отправляет напоминание.
    Выполняется только в пятницу. Если нет планов вообще (ни дома, ни в кино), отправляет раз в неделю."""
    if not bot:
        return
    
    conn_local = None
    cursor_local = None
    
    try:
        conn_local = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor_local = conn_local.cursor()
        
        now = datetime.now(PLANS_TZ)
        current_weekday = now.weekday()
        
        if current_weekday != 4:  # пятница
            return
        
        with db_lock:
            cursor_local.execute("SELECT DISTINCT chat_id FROM movies")
            chat_rows = cursor_local.fetchall()
        
        for row in chat_rows:
            chat_id = row['chat_id']
            
            try:
                chat_info = bot.get_chat(chat_id)
                if chat_info.type == 'private':
                    continue
            except Exception as e:
                logger.warning(f"[PREMIERE REMINDER] Не удалось получить информацию о чате {chat_id}: {e}")
                continue
            
            if not get_random_events_enabled(chat_id):
                continue
            
            if was_event_sent_today(chat_id, 'random_event') or was_event_sent_today(chat_id, 'weekend_reminder') or was_event_sent_today(chat_id, 'premiere_reminder'):
                logger.info(f"[PREMIERE REMINDER] Пропуск чата {chat_id} - уже было отправлено событие сегодня")
                continue
            
            with db_lock:
                cursor_local.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'reminder_cinema_premieres_disabled'", (chat_id,))
                reminder_disabled_row = cursor_local.fetchone()
            if reminder_disabled_row and reminder_disabled_row['value'] == 'true':
                continue
            
            with db_lock:
                cursor_local.execute('''
                    SELECT MAX(plan_datetime) FROM plans
                    WHERE chat_id = %s AND plan_type = 'cinema'
                ''', (chat_id,))
                last_cinema_row = cursor_local.fetchone()
            
            has_recent_cinema_plan = False
            if last_cinema_row and last_cinema_row['max']:
                last_cinema = last_cinema_row['max']
                if isinstance(last_cinema, str):
                    last_cinema = datetime.fromisoformat(last_cinema.replace('Z', '+00:00'))
                if last_cinema.tzinfo is None:
                    last_cinema = pytz.utc.localize(last_cinema)
                last_cinema = last_cinema.astimezone(PLANS_TZ)
                if (now - last_cinema).days < 14:
                    has_recent_cinema_plan = True
            
            if has_recent_cinema_plan:
                continue
            
            with db_lock:
                cursor_local.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'last_cinema_reminder_date'", (chat_id,))
                last_reminder_row = cursor_local.fetchone()
            
            should_send = True
            if last_reminder_row:
                last_reminder_str = last_reminder_row['value']
                try:
                    last_reminder = datetime.strptime(last_reminder_str, '%Y-%m-%d').date()
                    days_passed = (now.date() - last_reminder).days
                    
                    friday = now.replace(hour=0, minute=0, second=0, microsecond=0)
                    sunday = now.replace(hour=23, minute=59, second=59, microsecond=0) + timedelta(days=2)
                    with db_lock:
                        cursor_local.execute('''
                            SELECT COUNT(*) FROM plans
                            WHERE chat_id = %s 
                            AND plan_type = 'home'
                            AND plan_datetime >= %s 
                            AND plan_datetime <= %s
                        ''', (chat_id, friday, sunday))
                        home_plans_count = cursor_local.fetchone()
                    home_count = home_plans_count['count'] if home_plans_count else 0
                    
                    if home_count == 0 and days_passed < 7:
                        should_send = False
                except:
                    pass
            
            if should_send:
                try:
                    from moviebot.api.kinopoisk_api import get_premieres_for_period
                    
                    premieres = get_premieres_for_period('current_month')
                    
                    if premieres:
                        text = "🎬 Вы давно ничего не добавляли к просмотру в кинотеатре! Посмотрите, что сейчас идет в кино:\n\n"
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
                        
                        mark_event_sent(chat_id, 'premiere_reminder')
                        
                        with db_lock:
                            cursor_local.execute('''
                                INSERT INTO settings (chat_id, key, value)
                                VALUES (%s, 'last_cinema_reminder_date', %s)
                                ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                            ''', (chat_id, now.date().isoformat()))
                            conn_local.commit()
                        
                        logger.info(f"[PREMIERE REMINDER] Отправлено напоминание о премьерах для чата {chat_id}")
                except Exception as e:
                    logger.error(f"[PREMIERE REMINDER] Ошибка при отправке напоминания: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[PREMIERE REMINDER] Ошибка в check_premiere_reminder: {e}", exc_info=True)
    finally:
        if cursor_local:
            try:
                cursor_local.close()
            except:
                pass
        if conn_local:
            try:
                conn_local.close()
            except:
                pass

def choose_random_participant():
    """Раз в две недели выбирает случайного участника для выбора фильма"""
    if not bot:
        return
    
    conn_local = None
    cursor_local = None
    
    try:
        conn_local = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor_local = conn_local.cursor()
        
        now = datetime.now(PLANS_TZ)
        
        # Получаем все групповые чаты
        with db_lock:
            cursor_local.execute("SELECT DISTINCT chat_id FROM movies")
            chat_rows = cursor_local.fetchall()
        
        for row in chat_rows:
            chat_id = row['chat_id']  # RealDictCursor возвращает dict
            
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
            with db_lock:
                cursor_local.execute(
                    "SELECT value FROM settings WHERE chat_id = %s AND key = 'last_random_participant_date'",
                    (chat_id,)
                )
                last_date_row = cursor_local.fetchone()
            
            if last_date_row:
                last_date_str = last_date_row['value']
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
                
            with db_lock:
                cursor_local.execute(query, params)
                participants = cursor_local.fetchall()
            
            if not participants:
                continue
            
            # Проверка недели участия
            with db_lock:
                cursor_local.execute('''
                    SELECT user_id, MIN(timestamp) as first_participation
                    FROM stats
                    WHERE chat_id = %s
                    GROUP BY user_id
                ''', (chat_id,))
                first_participations = {
                    row['user_id']: row['first_participation']
                    for row in cursor_local.fetchall()
                }
            
            week_ago = now - timedelta(days=7)
            all_participated_week_ago = True
            for participant in participants:
                user_id = participant['user_id']
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
            user_id = participant['user_id']
            username = participant['username']
            
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
                                    cursor_local.execute(f"""
                                        UPDATE {table}
                                        SET chat_id = %s
                                        WHERE chat_id = %s
                                    """, (new_chat_id, original_chat_id))
                                
                                conn_local.commit()
                            
                            logger.info(f"[RANDOM PARTICIPANT] Обновлено записей chat_id")
                            
                            # Меняем chat_id для повторной попытки
                            chat_id = new_chat_id
                            
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
                    cursor_local.execute('''
                        INSERT INTO settings (chat_id, key, value)
                        VALUES (%s, 'last_random_participant_date', %s)
                        ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                    ''', (chat_id, now.date().isoformat()))
                    conn_local.commit()
                
                logger.info(f"[RANDOM PARTICIPANT] Успешно выбран участник {user_id} для чата {chat_id}")
            else:
                logger.warning(f"[RANDOM PARTICIPANT] Не удалось отправить сообщение в чат {original_chat_id}")
                
    except Exception as e:
        logger.error(f"[RANDOM PARTICIPANT] Глобальная ошибка в choose_random_participant: {e}", exc_info=True)
    finally:
        if cursor_local:
            try:
                cursor_local.close()
            except:
                pass
        if conn_local:
            try:
                conn_local.close()
            except:
                pass

def start_dice_game():
    """Раз в две недели запускает игру в кубик для выбора фильма - использует общую функцию send_dice_game_event"""
    if not bot:
        return
    
    conn_local = None
    cursor_local = None
    
    try:
        conn_local = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor_local = conn_local.cursor()
        
        now = datetime.now(PLANS_TZ)
        
        # Получаем все групповые чаты
        with db_lock:
            cursor_local.execute("SELECT DISTINCT chat_id FROM movies")
            chat_rows = cursor_local.fetchall()
        
        for row in chat_rows:
            chat_id = row['chat_id']  # RealDictCursor возвращает dict
            
            # Проверяем, было ли уже отправлено какое-то событие сегодня
            if was_event_sent_today(chat_id, 'random_event') or \
               was_event_sent_today(chat_id, 'weekend_reminder') or \
               was_event_sent_today(chat_id, 'premiere_reminder'):
                logger.info(f"[DICE GAME] Пропуск чата {chat_id} - уже было отправлено событие сегодня")
                continue
            
            # Используем общую функцию для отправки события
            from moviebot.utils.random_events import send_dice_game_event
            send_dice_game_event(chat_id, skip_checks=False)
            
    except Exception as e:
        logger.error(f"[DICE GAME] Критическая ошибка в start_dice_game: {e}", exc_info=True)
    finally:
        if cursor_local:
            try:
                cursor_local.close()
            except:
                pass
        if conn_local:
            try:
                conn_local.close()
            except:
                pass

def update_series_status_cache():
    """Фоновая задача: обновляет статусы сериалов раз в день"""
    logger.info("[CACHE] Запуск обновления кэша сериалов")
    
    conn_local = None
    cursor_local = None
    
    try:
        conn_local = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
        cursor_local = conn_local.cursor()
        
        with db_lock:
            cursor_local.execute("""
                SELECT DISTINCT kp_id, chat_id
                FROM movies
                WHERE is_series = 1
                  AND (last_api_update IS NULL OR last_api_update < NOW() - INTERVAL '1 day')
                LIMIT 30
            """)
            rows = cursor_local.fetchall()
        
        if not rows:
            logger.info("[CACHE] Нет сериалов для обновления кэша")
            return

        for row in rows:
            kp_id = row['kp_id']
            chat_id = row['chat_id']
            
            if kp_id is None:
                logger.warning(f"[CACHE] Пропущена запись с kp_id=None: {row}")
                continue
            
            try:
                # Основная логика: получаем актуальные данные из Kinopoisk API
                is_airing, next_ep = get_series_airing_status(kp_id)
                seasons_data = get_seasons_data(kp_id)
                seasons_count = len(seasons_data) if seasons_data else 0
                next_ep_json = json.dumps(next_ep) if next_ep else None

                # Обновляем — отдельный короткий conn + lock
                conn_update = psycopg2.connect(DATABASE_URL)
                cursor_update = conn_update.cursor()
                try:
                    with db_lock:
                        cursor_update.execute("""
                            UPDATE movies
                            SET is_ongoing = %s, 
                                seasons_count = %s, 
                                next_episode = %s, 
                                last_api_update = NOW()
                            WHERE chat_id = %s AND kp_id = %s
                        """, (is_airing, seasons_count, next_ep_json, chat_id, kp_id))
                    conn_update.commit()
                except Exception as db_e:
                    logger.error(f"[CACHE] Ошибка при обновлении сериала {kp_id}: {db_e}", exc_info=True)
                    try:
                        conn_update.rollback()
                    except:
                        pass
                finally:
                    try:
                        cursor_update.close()
                    except:
                        pass
                    try:
                        conn_update.close()
                    except:
                        pass
                
                logger.info(f"[CACHE] Обновлён кэш для kp_id={kp_id} (chat_id={chat_id}), seasons={seasons_count}, ongoing={is_airing}")

            except Exception as e:
                logger.error(f"[CACHE] Ошибка обновления kp_id={kp_id} (chat_id={chat_id}): {e}", exc_info=True)
        
        logger.info("[CACHE] Обновление кэша сериалов завершено")
        
    except Exception as e:
        logger.error(f"[CACHE] Глобальная ошибка в update_series_status_cache: {e}", exc_info=True)
    finally:
        if cursor_local:
            try:
                cursor_local.close()
            except:
                pass
        if conn_local:
            try:
                conn_local.close()
            except:
                pass