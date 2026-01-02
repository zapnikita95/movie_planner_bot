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
from database.db_operations import print_daily_stats

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()
plans_tz = PLANS_TZ  # Для обратной совместимости

# bot будет импортирован из moviebot.py при использовании
bot = None

def set_bot_instance(bot_instance):
    """Устанавливает экземпляр бота для использования в задачах"""
    global bot
    bot = bot_instance

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

                       m.title, m.link

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

            else:

                plan_id = plan[0]

                chat_id = plan[1]

                film_id = plan[2]

                plan_type = plan[3]

                plan_datetime = plan[4]

                user_id = plan[5]

                title = plan[6]

                link = plan[7]

            

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

                # 1. Утреннее напоминание в 9:00 в день сеанса

                morning_dt = plan_dt_local.replace(hour=9, minute=0)

                morning_utc = morning_dt.astimezone(pytz.utc)

                

                # Планируем утреннее напоминание, если оно еще не запланировано и время еще не прошло

                if morning_utc > now_utc:

                    try:

                        job_id = f'plan_morning_{chat_id}_{plan_id}_{int(morning_utc.timestamp())}'

                        existing_job = scheduler.get_job(job_id)

                        if not existing_job:

                            scheduler.add_job(

                                send_plan_notification,

                                'date',

                                run_date=morning_utc,

                                args=[chat_id, film_id, title, link, plan_type],

                                id=job_id

                            )

                            logger.info(f"[PLAN CHECK] Запланировано утреннее уведомление для плана {plan_id} (фильм {title}) на {morning_utc}")

                    except Exception as e:

                        logger.warning(f"[PLAN CHECK] Не удалось запланировать утреннее уведомление для плана {plan_id}: {e}")

                elif morning_utc <= now_utc and morning_utc >= now_utc - timedelta(minutes=30):

                    # Время утреннего напоминания уже прошло, но не более 30 минут назад - отправляем сразу

                    try:

                        send_plan_notification(chat_id, film_id, title, link, plan_type, plan_id=plan_id)

                        logger.info(f"[PLAN CHECK] Утреннее уведомление отправлено сразу для плана {plan_id} (фильм {title})")

                    except Exception as e:

                        logger.error(f"[PLAN CHECK] Ошибка отправки утреннего уведомления для плана {plan_id}: {e}", exc_info=True)

                

                # 2. Напоминание за 10 минут до сеанса с билетами

                ticket_dt = plan_dt_local - timedelta(minutes=10)

                ticket_utc = ticket_dt.astimezone(pytz.utc)

                

                # Проверяем наличие билетов

                with db_lock:

                    cursor.execute('SELECT COUNT(*) FROM tickets WHERE plan_id = %s', (plan_id,))

                    ticket_count_row = cursor.fetchone()

                    ticket_count = ticket_count_row[0] if ticket_count_row else 0

                

                if ticket_count > 0:

                    # Планируем напоминание с билетами, если оно еще не запланировано и время еще не прошло

                    if ticket_utc > now_utc:

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

                        try:

                            send_ticket_notification(chat_id, plan_id)

                            logger.info(f"[PLAN CHECK] Уведомление с билетами отправлено сразу для плана {plan_id} (фильм {title})")

                        except Exception as e:

                            logger.error(f"[PLAN CHECK] Ошибка отправки уведомления с билетами для плана {plan_id}: {e}", exc_info=True)

            else:

                # Для планов дома проверяем, нужно ли отправить уведомление на время плана

                # (время уже наступило или прошло не более 30 минут назад)

                if plan_datetime <= now_utc and plan_datetime >= now_utc - timedelta(minutes=30):

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

    except Exception as e:

        logger.error(f"[PLAN CHECK] Ошибка при проверке планов: {e}", exc_info=True)



# Настройка периодического вывода статистики
# Вызовы scheduler.add_job должны быть в moviebot.py после импорта модуля



# Очистка планов

def clean_home_plans():
    """Ежедневно удаляет планы дома на вчерашний день, если по фильму нет оценок"""

    yesterday = (datetime.now(plans_tz) - timedelta(days=1)).date()

    

    with db_lock:

        # Находим планы дома на вчера (используем AT TIME ZONE для корректной работы с TIMESTAMP WITH TIME ZONE)

        cursor.execute('''

            SELECT p.id, p.film_id, p.chat_id

            FROM plans p

            WHERE p.plan_type = 'home' AND DATE(p.plan_datetime AT TIME ZONE 'Europe/Moscow') = %s

        ''', (yesterday,))

        rows = cursor.fetchall()

        

        deleted_count = 0

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


