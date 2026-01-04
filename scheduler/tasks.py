"""
Модуль для задач планировщика
"""
import logging
from datetime import datetime, timedelta
import pytz
import json
from database.db_connection import get_db_connection, get_db_cursor, db_lock
from config.settings import PLANS_TZ
from bot.states import plan_notification_messages
from database.db_operations import print_daily_stats, get_user_timezone_or_default, get_notification_settings

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()
plans_tz = PLANS_TZ  # Для обратной совместимости

# bot и scheduler будут импортированы из moviebot.py при использовании
bot = None
scheduler = None

def set_bot_instance(bot_instance):
    """Устанавливает экземпляр бота для использования в задачах"""
    global bot
    bot = bot_instance

def set_scheduler_instance(scheduler_instance):
    """Устанавливает экземпляр scheduler для использования в задачах"""
    global scheduler
    scheduler = scheduler_instance

def hourly_stats():

    """Вызывается каждый час для вывода статистики"""

    print_daily_stats()



# Функции для уведомлений о планах (определяем до использования в scheduler)

def send_plan_notification(chat_id, film_id, title, link, plan_type, plan_id=None):
    """Отправляет уведомление о запланированном просмотре"""

    try:

        plan_type_text = "дома" if plan_type == 'home' else "в кино"

        text = f"🔔 Напоминание: сегодня запланирован просмотр {plan_type_text}!\n\n"

        text += f"<b>{title}</b>\n{link}"

        msg = bot.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False)

        # Сохраняем message_id для обработки реакций (сохраняем link, film_id и plan_id)

        plan_notification_messages[msg.message_id] = {

            'link': link,

            'film_id': film_id,

            'plan_id': plan_id

        }

        logger.info(f"[PLAN NOTIFICATION] Уведомление отправлено для фильма {title} в чат {chat_id}, message_id={msg.message_id}, plan_id={plan_id}")

        

        # Отмечаем как отправленное в базе данных, если plan_id передан

        if plan_id:

            try:

                with db_lock:

                    cursor.execute('''

                        UPDATE plans 

                        SET notification_sent = TRUE 

                        WHERE id = %s

                    ''', (plan_id,))

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
            ticket_file_id = ticket_row[0]
            title = ticket_row[1]
            plan_dt_value = ticket_row[2]
        
        if not ticket_file_id:
            logger.warning(f"[TICKET NOTIFICATION] Билеты не найдены для plan_id={plan_id}")
            return
        
        text = f"🎟️ <b>Напоминание: через 10 минут сеанс!</b>\n\n<b>{title}</b>\n\nВаши билеты:"
        
        try:
            bot.send_photo(chat_id, ticket_file_id, caption=text, parse_mode='HTML')
        except:
            try:
                bot.send_document(chat_id, ticket_file_id, caption=text, parse_mode='HTML')
            except Exception as e:
                logger.error(f"[TICKET NOTIFICATION] Ошибка отправки билетов: {e}")
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

        

        with db_lock:

            cursor.execute('''

                SELECT p.id, p.chat_id, p.film_id, p.plan_type, p.plan_datetime, p.user_id,

                       m.title, m.link, p.notification_sent, p.ticket_notification_sent

                FROM plans p

                JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id

                WHERE p.plan_datetime >= %s 

                  AND p.plan_datetime <= %s

            ''', (check_start, check_end))

            plans = cursor.fetchall()

        

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
                                send_plan_notification(chat_id, film_id, title, link, plan_type, plan_id=plan_id)
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

                # Для планов дома проверяем два типа уведомлений:

                # 1. Напоминание в день просмотра (только если это сегодня)
                # Время зависит от дня недели: будни 19:00, выходные 9:00

                if plan_dt_local.date() == now_local.date():
                    # Получаем настройки времени напоминаний
                    notify_settings = get_notification_settings(chat_id)
                    
                    # Определяем, будний день или выходной (0 = понедельник, 6 = воскресенье)
                    weekday = plan_dt_local.weekday()  # 0-6, где 0 = понедельник, 6 = воскресенье
                    is_weekend = weekday >= 5  # Суббота (5) и воскресенье (6)
                    
                    # Если разделение на будни/выходные отключено, используем настройки для будних дней
                    if notify_settings.get('separate_weekdays') == 'false':
                        reminder_hour = notify_settings.get('home_weekday_hour', 19)
                        reminder_minute = notify_settings.get('home_weekday_minute', 0)
                    elif is_weekend:
                        reminder_hour = notify_settings.get('home_weekend_hour', 9)
                        reminder_minute = notify_settings.get('home_weekend_minute', 0)
                    else:
                        reminder_hour = notify_settings.get('home_weekday_hour', 19)
                        reminder_minute = notify_settings.get('home_weekday_minute', 0)

                    reminder_dt = plan_dt_local.replace(hour=reminder_hour, minute=reminder_minute)

                    reminder_utc = reminder_dt.astimezone(pytz.utc)

                    

                    # Планируем напоминание, если оно еще не запланировано и время еще не прошло
                    # Проверяем, не было ли уже отправлено уведомление
                    if reminder_utc > now_utc and not notification_sent:

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

                                logger.info(f"[PLAN CHECK] Запланировано напоминание для плана дома {plan_id} (фильм {title}) на {reminder_utc} ({reminder_hour}:{reminder_minute:02d})")

                        except Exception as e:

                            logger.warning(f"[PLAN CHECK] Не удалось запланировать напоминание для плана {plan_id}: {e}")

                    elif reminder_utc <= now_utc and reminder_utc >= now_utc - timedelta(minutes=30):

                        # Время напоминания уже прошло, но не более 30 минут назад - отправляем сразу
                        # Проверяем, не было ли уже отправлено уведомление
                        if not notification_sent:
                            try:
                                # Проверяем, не запланировано ли уже уведомление
                                job_id = f'plan_reminder_{chat_id}_{plan_id}_{int(reminder_utc.timestamp())}'
                                existing_job = scheduler.get_job(job_id)
                                if not existing_job:
                                    send_plan_notification(chat_id, film_id, title, link, plan_type, plan_id=plan_id)
                                    logger.info(f"[PLAN CHECK] Напоминание отправлено сразу для плана дома {plan_id} (фильм {title})")
                                else:
                                    logger.info(f"[PLAN CHECK] Напоминание уже запланировано для плана дома {plan_id}")
                            except Exception as e:
                                logger.error(f"[PLAN CHECK] Ошибка отправки напоминания для плана {plan_id}: {e}", exc_info=True)
                        else:
                            logger.info(f"[PLAN CHECK] Напоминание уже отправлено для плана дома {plan_id}, пропускаем")

                

                # 2. Напоминание на время плана (время уже наступило или прошло не более 30 минут назад)

                if plan_datetime <= now_utc and plan_datetime >= now_utc - timedelta(minutes=30):

                    # Проверяем, не было ли уже отправлено уведомление
                    if not notification_sent:
                        try:

                            # Проверяем, не было ли уже запланировано это уведомление

                            job_id = f'plan_notify_{chat_id}_{film_id}_{int(plan_datetime.timestamp())}'

                            existing_job = scheduler.get_job(job_id)

                            if not existing_job:

                                # Отправляем уведомление сразу, так как время уже наступило

                                send_plan_notification(chat_id, film_id, title, link, plan_type, plan_id=plan_id)

                                logger.info(f"[PLAN CHECK] Уведомление отправлено для плана {plan_id} (фильм {title})")

                        except Exception as e:

                            logger.error(f"[PLAN CHECK] Ошибка отправки уведомления для плана {plan_id}: {e}", exc_info=True)
                    else:
                        logger.info(f"[PLAN CHECK] Уведомление уже отправлено для плана {plan_id}, пропускаем")

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
        text += f"<a href='https://www.kinopoisk.ru/series/{kp_id}/'>Кинопоиск</a>"
        
        # Отправляем уведомление всем подписанным пользователям
        with db_lock:
            cursor.execute('''
                SELECT DISTINCT user_id 
                FROM series_subscriptions 
                WHERE chat_id = %s AND film_id = %s AND subscribed = TRUE
            ''', (chat_id, film_id))
            subscribers = cursor.fetchall()
        
        subscribers_list = []
        for sub_row in subscribers:
            user_id = sub_row.get('user_id') if isinstance(sub_row, dict) else sub_row[0]
            subscribers_list.append(user_id)
            try:
                bot.send_message(chat_id, text, parse_mode='HTML')
                logger.info(f"[SERIES NOTIFICATION] Уведомление отправлено для сериала {title} (kp_id={kp_id})")
            except Exception as e:
                logger.error(f"[SERIES NOTIFICATION] Ошибка отправки уведомления: {e}")
        
        # После отправки уведомления проверяем, есть ли следующая серия
        from api.kinopoisk_api import get_seasons_data
        seasons = get_seasons_data(kp_id)
        
        if seasons:
            from datetime import datetime as dt, timedelta
            import pytz
            now = dt.now()
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
        
        from api.kinopoisk_api import get_seasons_data
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
        from datetime import datetime as dt
        now = dt.now()
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
            from datetime import timedelta
            import pytz
            
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

            rating_messages[msg.message_id] = film_id

            logger.info(f"[RATING REMINDER] Напоминание отправлено user_id={user_id}, film_id={film_id}, message_id={msg.message_id}")

    except Exception as e:

        logger.error(f"[RATING REMINDER] Ошибка при отправке напоминания: {e}", exc_info=True)


def check_subscription_payments():
    """Проверяет подписки и отправляет уведомления за день до списания"""
    if not bot:
        return
    
    try:
        from datetime import datetime, timedelta
        import pytz
        from database.db_operations import get_active_subscription
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
        
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


def process_recurring_payments():
    """Выполняет безакцептные списания для подписок с payment_method_id"""
    if not bot:
        return
    
    try:
        from yookassa import Configuration, Payment
        from config.settings import YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY
        import uuid as uuid_module
        from database.db_operations import renew_subscription, save_payment, update_payment_status, create_subscription
        
        Configuration.account_id = YOOKASSA_SHOP_ID
        Configuration.secret_key = YOOKASSA_SECRET_KEY
        
        now = datetime.now(pytz.UTC)
        
        # Находим подписки, у которых next_payment_date сегодня и есть payment_method_id
        with db_lock:
            cursor.execute("""
                SELECT id, chat_id, user_id, subscription_type, plan_type, period_type, price, 
                       next_payment_date, payment_method_id, telegram_username, group_username, group_size
                FROM subscriptions
                WHERE is_active = TRUE
                AND next_payment_date IS NOT NULL
                AND payment_method_id IS NOT NULL
                AND DATE(next_payment_date AT TIME ZONE 'UTC') = DATE(%s AT TIME ZONE 'UTC')
            """, (now,))
            subscriptions = cursor.fetchall()
        
        for sub in subscriptions:
            try:
                subscription_id = sub.get('id') if isinstance(sub, dict) else sub[0]
                chat_id = sub.get('chat_id') if isinstance(sub, dict) else sub[1]
                user_id = sub.get('user_id') if isinstance(sub, dict) else sub[2]
                subscription_type = sub.get('subscription_type') if isinstance(sub, dict) else sub[3]
                plan_type = sub.get('plan_type') if isinstance(sub, dict) else sub[4]
                period_type = sub.get('period_type') if isinstance(sub, dict) else sub[5]
                price = float(sub.get('price') if isinstance(sub, dict) else sub[6])
                payment_method_id = sub.get('payment_method_id') if isinstance(sub, dict) else sub[8]
                telegram_username = sub.get('telegram_username') if isinstance(sub, dict) else sub[9]
                group_username = sub.get('group_username') if isinstance(sub, dict) else sub[10]
                group_size = sub.get('group_size') if isinstance(sub, dict) else sub[11]
                
                logger.info(f"[RECURRING PAYMENT] Обработка подписки {subscription_id}, payment_method_id={payment_method_id}, сумма={price}")
                
                # Создаем безакцептный платеж
                payment_id = str(uuid_module.uuid4())
                
                plan_names = {
                    'notifications': 'Уведомления о сериалах',
                    'recommendations': 'Персональные рекомендации',
                    'tickets': 'Билеты в кино',
                    'all': 'Все режимы'
                }
                plan_name = plan_names.get(plan_type, plan_type)
                
                period_names = {
                    'month': 'месяц',
                    '3months': '3 месяца',
                    'year': 'год'
                }
                period_name = period_names.get(period_type, period_type)
                
                subscription_type_name = 'Личная подписка' if subscription_type == 'personal' else f'Групповая подписка (на {group_size} участников)'
                description = f"{subscription_type_name}: {plan_name}, период: {period_name} (User ID: {user_id})"
                
                metadata = {
                    "user_id": str(user_id),
                    "chat_id": str(chat_id),
                    "subscription_type": subscription_type,
                    "plan_type": plan_type,
                    "period_type": period_type,
                    "payment_id": payment_id,
                    "recurring": "true"
                }
                if group_size:
                    metadata["group_size"] = str(group_size)
                if telegram_username:
                    metadata["telegram_username"] = telegram_username
                if group_username:
                    metadata["group_username"] = group_username
                
                # Создаем безакцептный платеж используя сохраненный payment_method_id
                payment = Payment.create({
                    "amount": {
                        "value": f"{price:.2f}",
                        "currency": "RUB"
                    },
                    "payment_method_id": payment_method_id,  # Используем сохраненный способ оплаты
                    "capture": True,
                    "description": description,
                    "metadata": metadata
                })
                
                logger.info(f"[RECURRING PAYMENT] Платеж создан: {payment.id}, статус: {payment.status}")
                
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
                    
                    # Отправляем уведомление пользователю
                    text = "✅ <b>Автоматическое списание выполнено</b>\n\n"
                    text += f"Подписка продлена на {period_name}.\n"
                    text += f"💰 Сумма: <b>{price}₽</b>\n\n"
                    text += "Спасибо за использование нашего сервиса! 🎉"
                    
                    try:
                        bot.send_message(chat_id, text, parse_mode='HTML')
                        logger.info(f"[RECURRING PAYMENT] Уведомление отправлено для подписки {subscription_id}")
                    except Exception as e:
                        logger.error(f"[RECURRING PAYMENT] Ошибка отправки уведомления: {e}")
                else:
                    logger.warning(f"[RECURRING PAYMENT] Платеж {payment.id} не успешен, статус: {payment.status}")
                    # Отправляем уведомление об ошибке
                    text = "⚠️ <b>Ошибка автоматического списания</b>\n\n"
                    text += f"Не удалось списать оплату за подписку.\n"
                    text += f"Статус: {payment.status}\n\n"
                    text += "Пожалуйста, проверьте способ оплаты или обратитесь в поддержку."
                    
                    try:
                        bot.send_message(chat_id, text, parse_mode='HTML')
                    except Exception as e:
                        logger.error(f"[RECURRING PAYMENT] Ошибка отправки уведомления об ошибке: {e}")
                
            except Exception as e:
                logger.error(f"[RECURRING PAYMENT] Ошибка обработки подписки {subscription_id}: {e}", exc_info=True)
    
    except Exception as e:
        logger.error(f"[RECURRING PAYMENT] Ошибка обработки рекуррентных платежей: {e}", exc_info=True)


