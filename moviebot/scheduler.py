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
from moviebot.database.db_connection import db_lock
from moviebot.config import PLANS_TZ, DATABASE_URL

# Локальные соединения: scheduler не использует глобальные get_db_connection/get_db_cursor
def _scheduler_conn():
    """Новое соединение для каждой операции (не глобальное)."""
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
from moviebot.api.kinopoisk_api import get_seasons_data
from moviebot.api.kinopoisk_api import get_external_sources

# Импорт helpers отключён полностью — все нужные функции определены в этом же файле (scheduler.py)
# from moviebot.utils.helpers import (...)
from moviebot.database.db_operations import get_user_timezone_or_default, get_notification_settings
from moviebot.bot.handlers.seasons import get_series_airing_status
from moviebot.utils.helpers import has_notifications_access, has_series_features_access

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
    # Локальное соединение (не глобальное)
    conn_local = _scheduler_conn()
    cursor_local = None
    
    try:
        plan_type_text = "дома" if plan_type == 'home' else "в кино"
        text = f"🔔 Напоминание: сегодня запланирован просмотр {plan_type_text}!\n\n"
        text += f"<b>{title}</b>\n{link}"
       
        markup = None
        kp_id = None  # Будем получать kp_id для кнопок
       
        # Проверяем, является ли фильм сериалом, и получаем информацию о последней просмотренной серии
        is_series = False
        last_episode_info = None
        if user_id and film_id:
            conn_series = _scheduler_conn()
            cursor_series = None
            try:
                with db_lock:
                    cursor_series = conn_series.cursor()
                    cursor_series.execute('SELECT is_series FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                    movie_row = cursor_series.fetchone()
                    if movie_row:
                        is_series = bool(movie_row.get('is_series') if isinstance(movie_row, dict) else movie_row[0])
                       
                        if is_series:
                            cursor_series.execute('''
                                SELECT season_number, episode_number
                                FROM series_tracking
                                WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                                ORDER BY season_number DESC, episode_number DESC
                                LIMIT 1
                            ''', (chat_id, film_id, user_id))
                            last_episode_row = cursor_series.fetchone()
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
            finally:
                if cursor_series:
                    try:
                        cursor_series.close()
                    except:
                        pass
                try:
                    conn_series.close()
                except:
                    pass
       
        if is_series and last_episode_info:
            text += f"\n\n📺 <b>Последняя просмотренная серия:</b> Сезон {last_episode_info['season']}, Серия {last_episode_info['episode']}"
       
        has_access = False
        if user_id and film_id:
            has_access = has_series_features_access(chat_id, user_id, film_id)
       
        if not has_access and user_id:
            text += "\n\n💡 <b>Вы можете отслеживать просмотренные серии и подключить напоминания о выходе новых серий с подпиской 💎 Movie Planner PRO</b>"
       
        # Для планов "дома" — существующий код с онлайн-кинотеатрами
        if plan_type == 'home' and plan_id:
            conn_plan = _scheduler_conn()
            cursor_plan = None
            plan_row = None
            try:
                with db_lock:
                    cursor_plan = conn_plan.cursor()
                    cursor_plan.execute('''
                        SELECT streaming_service, streaming_url, streaming_done, ticket_file_id
                        FROM plans
                        WHERE id = %s AND chat_id = %s
                    ''', (plan_id, chat_id))
                    plan_row = cursor_plan.fetchone()
            finally:
                if cursor_plan:
                    try:
                        cursor_plan.close()
                    except:
                        pass
                try:
                    conn_plan.close()
                except:
                    pass
            
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
                    conn_kp = _scheduler_conn()
                    cursor_kp = None
                    kp_id = None
                    try:
                        with db_lock:
                            cursor_kp = conn_kp.cursor()
                            cursor_kp.execute('SELECT kp_id FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                            movie_row = cursor_kp.fetchone()
                            if movie_row:
                                kp_id = movie_row.get('kp_id') if isinstance(movie_row, dict) else movie_row[0]
                    finally:
                        if cursor_kp:
                            try:
                                cursor_kp.close()
                            except:
                                pass
                        try:
                            conn_kp.close()
                        except:
                            pass
                    
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
            conn_cinema = _scheduler_conn()
            cursor_cinema = None
            try:
                with db_lock:
                    cursor_cinema = conn_cinema.cursor()
                    cursor_cinema.execute('SELECT ticket_file_id FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
                    row = cursor_cinema.fetchone()
                    ticket_file_id = None
                    if row:
                        if isinstance(row, dict):
                            ticket_file_id = row.get('ticket_file_id')
                        else:
                            ticket_file_id = row[0]
            finally:
                if cursor_cinema:
                    try:
                        cursor_cinema.close()
                    except:
                        pass
                try:
                    conn_cinema.close()
                except:
                    pass
            
            if not markup:
                markup = InlineKeyboardMarkup()
           
            if not ticket_file_id or str(ticket_file_id).strip() == '' or ticket_file_id == 'null':
                markup.add(InlineKeyboardButton("📸 Добавить билеты", callback_data=f"add_ticket:{plan_id}"))
                text += "\n\n🎟 Не забудьте добавить фото билетов!"
                logger.info(f"[PLAN NOTIFICATION] Кнопка 'Добавить билеты' для плана {plan_id}")
            else:
                markup.add(InlineKeyboardButton("🎟 Показать билеты", callback_data=f"show_ticket:{plan_id}"))
                logger.info(f"[PLAN NOTIFICATION] Кнопка 'Показать билеты' для плана {plan_id}")

        # Получаем kp_id для кнопок "Перейти к описанию" и "Изменить в расписании"
        if film_id and plan_id:
            conn_kp = _scheduler_conn()
            cursor_kp = None
            try:
                with db_lock:
                    cursor_kp = conn_kp.cursor()
                    cursor_kp.execute('SELECT kp_id FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                    movie_row = cursor_kp.fetchone()
                    if movie_row:
                        kp_id = movie_row.get('kp_id') if isinstance(movie_row, dict) else movie_row[0]
            finally:
                if cursor_kp:
                    try:
                        cursor_kp.close()
                    except:
                        pass
                try:
                    conn_kp.close()
                except:
                    pass
        
        # Добавляем кнопки "Перейти к описанию" и "Изменить в расписании", если есть plan_id и kp_id
        if plan_id and kp_id:
            if not markup:
                markup = InlineKeyboardMarkup(row_width=1)
            try:
                kp_id_int = int(kp_id)
                markup.add(InlineKeyboardButton("📖 Перейти к описанию", callback_data=f"back_to_film:{kp_id_int}"))
                markup.add(InlineKeyboardButton("✏️ Изменить в расписании", callback_data=f"edit_plan:{plan_id}"))
                logger.info(f"[PLAN NOTIFICATION] Добавлены кнопки 'Перейти к описанию' и 'Изменить в расписании' для плана {plan_id}")
            except (ValueError, TypeError) as e:
                logger.warning(f"[PLAN NOTIFICATION] Не удалось преобразовать kp_id в int: {kp_id}, ошибка: {e}")
        
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
       
        # КРИТИЧЕСКИ ВАЖНО: Обновляем флаг notification_sent СРАЗУ после отправки
        if plan_id:
            conn_update = _scheduler_conn()
            cursor_update = None
            try:
                with db_lock:
                    cursor_update = conn_update.cursor()
                    cursor_update.execute('UPDATE plans SET notification_sent = TRUE WHERE id = %s', (plan_id,))
                    conn_update.commit()
                logger.info(f"[PLAN NOTIFICATION] План {plan_id} отмечен как уведомление отправлено")
            except Exception as e:
                logger.error(f"[PLAN NOTIFICATION] Не удалось отметить план {plan_id} как отправленный: {e}", exc_info=True)
            finally:
                if cursor_update:
                    try:
                        cursor_update.close()
                    except:
                        pass
                try:
                    conn_update.close()
                except:
                    pass

    except Exception as e:
        logger.error(f"[PLAN NOTIFICATION] Ошибка отправки уведомления: {e}", exc_info=True)


def send_plan_notification_combined(chat_id, date_str, user_id=None):
    """Одно утреннее уведомление на день: список всех планов на дату с кнопками к описанию каждого фильма."""

    if not bot:
        return
    user_tz = get_user_timezone_or_default(user_id or chat_id if chat_id > 0 else 0)
    try:
        start_local = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=user_tz)
    except Exception:
        logger.warning(f"[PLAN COMBINED] Неверный date_str: {date_str}")
        return
    start_utc = start_local.astimezone(pytz.utc)
    end_utc = start_utc + timedelta(days=1)

    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
    try:
        with db_lock:
            cursor_local.execute('''
                SELECT p.id AS plan_id, p.chat_id, p.film_id, p.plan_type, p.plan_datetime,
                       p.user_id, p.ticket_file_id, p.streaming_service, p.streaming_url,
                       COALESCE(p.custom_title, m.title, 'Мероприятие') AS title, m.link, m.kp_id
                FROM plans p
                LEFT JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.chat_id = %s AND p.plan_datetime >= %s AND p.plan_datetime < %s
                  AND (p.notification_sent = FALSE OR p.notification_sent IS NULL)
                ORDER BY p.plan_datetime
            ''', (chat_id, start_utc, end_utc))
            rows = cursor_local.fetchall()
    except Exception as e:
        logger.error(f"[PLAN COMBINED] Ошибка запроса планов: {e}", exc_info=True)
        return
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass

    if not rows:
        return

    plans = []
    for r in rows:
        if isinstance(r, dict):
            plans.append({
                'plan_id': r.get('plan_id'), 'film_id': r.get('film_id'), 'plan_type': r.get('plan_type'),
                'plan_datetime': r.get('plan_datetime'), 'user_id': r.get('user_id'),
                'ticket_file_id': r.get('ticket_file_id'), 'streaming_service': r.get('streaming_service'),
                'streaming_url': r.get('streaming_url'), 'title': (r.get('title') or 'Мероприятие'),
                'link': r.get('link'), 'kp_id': r.get('kp_id')
            })
        else:
            plans.append({
                'plan_id': r[0], 'film_id': r[2], 'plan_type': r[3], 'plan_datetime': r[4], 'user_id': r[5],
                'ticket_file_id': r[6], 'streaming_service': r[7] if len(r) > 7 else None,
                'streaming_url': r[8] if len(r) > 8 else None, 'title': (r[9] if len(r) > 9 else None) or 'Мероприятие',
                'link': r[10] if len(r) > 10 else None, 'kp_id': r[11] if len(r) > 11 else None
            })

    import html as html_module
    single = len(plans) == 1
    p0 = plans[0]
    if single:
        plan_type_text = "дома" if p0['plan_type'] == 'home' else "в кино"
        dt0 = p0['plan_datetime']
        if hasattr(dt0, 'astimezone'):
            dt0_local = dt0.astimezone(user_tz) if dt0.tzinfo else user_tz.localize(dt0.replace(tzinfo=None))
        else:
            dt0_local = datetime.fromisoformat(str(dt0).replace('Z', '+00:00')).astimezone(user_tz)
        time_only = dt0_local.strftime('%H:%M')
        text = f"🔔 Напоминание: сегодня запланирован просмотр {plan_type_text} в {time_only}!\n\n"
    else:
        text = "🔔 На сегодня запланированы просмотры:\n\n"

    for p in plans:
        dt = p['plan_datetime']
        if hasattr(dt, 'astimezone'):
            dt_local = dt.astimezone(user_tz) if dt.tzinfo else user_tz.localize(dt.replace(tzinfo=None))
        else:
            dt_local = datetime.fromisoformat(str(dt).replace('Z', '+00:00')).astimezone(user_tz)
        time_str = dt_local.strftime('%d.%m %H:%M')
        title_short = (p.get('title') or '')[:50]
        if isinstance(title_short, str):
            title_esc = html_module.escape(title_short)
        else:
            title_esc = str(title_short)[:50]
        if p['plan_type'] == 'home':
            icon = '🏠'
        elif p.get('ticket_file_id') and str(p.get('ticket_file_id', '')).strip() and str(p.get('ticket_file_id')) != 'null':
            icon = '🎟️'
        else:
            icon = '🎥'
        text += f"{icon} {title_esc} — {time_str}\n"
    text += "\n🏠 — просмотр дома\n🎥 — просмотр в кино\n🎟️ — загружены билеты"
    if single and p0.get('link'):
        text += f"\n\n{p0.get('link')}"

    markup = InlineKeyboardMarkup(row_width=1)
    for p in plans:
        kp_id = p.get('kp_id')
        title_btn = (p.get('title') or 'Описание')[:30]
        if kp_id is not None:
            try:
                kp_int = int(kp_id)
                markup.add(InlineKeyboardButton(f"📖 {title_btn}", callback_data=f"back_to_film:{kp_int}"))
            except (ValueError, TypeError):
                pass
        else:
            markup.add(InlineKeyboardButton(f"📖 {title_btn}", callback_data=f"edit_plan:{p['plan_id']}"))

    if single:
        if p0['plan_type'] == 'cinema':
            if p0.get('ticket_file_id') and str(p0.get('ticket_file_id', '')).strip() and str(p0.get('ticket_file_id')) != 'null':
                markup.add(InlineKeyboardButton("🎟 Показать билеты", callback_data=f"show_ticket:{p0['plan_id']}"))
            else:
                markup.add(InlineKeyboardButton("📸 Добавить билеты", callback_data=f"add_ticket:{p0['plan_id']}"))
        elif p0['plan_type'] == 'home' and p0.get('streaming_service') and p0.get('streaming_url'):
            markup.add(InlineKeyboardButton(p0['streaming_service'], url=p0['streaming_url']))
        markup.add(InlineKeyboardButton("✏️ Изменить в расписании", callback_data=f"edit_plan:{p0['plan_id']}"))

    try:
        bot.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup)
    except Exception as e:
        logger.error(f"[PLAN COMBINED] Ошибка отправки: {e}", exc_info=True)
        return

    conn_update = _scheduler_conn()
    cursor_update = conn_update.cursor()
    try:
        with db_lock:
            for p in plans:
                cursor_update.execute('UPDATE plans SET notification_sent = TRUE WHERE id = %s AND chat_id = %s', (p['plan_id'], chat_id))
            conn_update.commit()
        logger.info(f"[PLAN COMBINED] Отправлено объединённое уведомление для {len(plans)} планов в чат {chat_id}")
    except Exception as e:
        logger.error(f"[PLAN COMBINED] Ошибка обновления notification_sent: {e}", exc_info=True)
    finally:
        try:
            cursor_update.close()
        except:
            pass
        try:
            conn_update.close()
        except:
            pass


def send_ticket_notification(chat_id, plan_id):
    """Отправляет напоминание с билетами за 10 минут до сеанса"""
    
    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
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
        
        conn_local = _scheduler_conn()
        cursor_local = conn_local.cursor()
        
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
                        cursor_local = conn_local.cursor()
                    
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

        # Группируем планы по (chat_id, дата в TZ пользователя) для одного утреннего уведомления на день
        groups = {}
        for plan in plans:
            if isinstance(plan, dict):
                plan_id, chat_id, film_id, plan_type, plan_datetime = plan.get('id'), plan.get('chat_id'), plan.get('film_id'), plan.get('plan_type'), plan.get('plan_datetime')
                user_id = plan.get('user_id')
            else:
                plan_id, chat_id, film_id, plan_type, plan_datetime = plan[0], plan[1], plan[2], plan[3], plan[4]
                user_id = plan[5] if len(plan) > 5 else None
            user_tz = get_user_timezone_or_default(user_id)
            if hasattr(plan_datetime, 'astimezone'):
                plan_dt_local = plan_datetime.astimezone(user_tz) if plan_datetime.tzinfo else user_tz.localize(plan_datetime.replace(tzinfo=None))
            else:
                plan_dt_local = datetime.fromisoformat(str(plan_datetime).replace('Z', '+00:00')).astimezone(user_tz)
            date_key = plan_dt_local.date()
            key = (chat_id, date_key.isoformat())
            if key not in groups:
                groups[key] = {'user_id': user_id, 'date_str': date_key.isoformat(), 'reminder_utc': None}
            if groups[key]['reminder_utc'] is None:
                tz_for_reminder = get_user_timezone_or_default(groups[key]['user_id'])
                now_local = datetime.now(tz_for_reminder)
                if date_key >= now_local.date():
                    notify_settings = get_notification_settings(chat_id)
                    wd = date_key.weekday()
                    is_weekend = wd >= 5
                    if notify_settings.get('separate_weekdays') == 'false':
                        h = notify_settings.get('cinema_weekday_hour', 9)
                        m = notify_settings.get('cinema_weekday_minute', 0)
                    elif is_weekend:
                        h, m = notify_settings.get('cinema_weekend_hour', 9), notify_settings.get('cinema_weekend_minute', 0)
                    else:
                        h, m = notify_settings.get('cinema_weekday_hour', 9), notify_settings.get('cinema_weekday_minute', 0)
                    reminder_local = tz_for_reminder.localize(datetime.combine(date_key, datetime.min.time().replace(hour=h, minute=m)))
                    groups[key]['reminder_utc'] = reminder_local.astimezone(pytz.utc)

        for key, g in groups.items():
            chat_id, date_str = key[0], g['date_str']
            reminder_utc = g.get('reminder_utc')
            user_id = g.get('user_id')
            if reminder_utc is None:
                continue
            diff = (reminder_utc - now_utc).total_seconds()
            if diff > 5:
                job_id = f'plan_reminder_combined_{chat_id}_{date_str}'
                try:
                    if not scheduler.get_job(job_id):
                        scheduler.add_job(
                            send_plan_notification_combined,
                            'date',
                            run_date=reminder_utc,
                            args=[chat_id, date_str],
                            kwargs={'user_id': user_id},
                            id=job_id
                        )
                        logger.info(f"[PLAN CHECK] Запланировано объединённое уведомление для чата {chat_id} на {date_str} в {reminder_utc}")
                except Exception as e:
                    logger.warning(f"[PLAN CHECK] Не удалось запланировать объединённое уведомление: {e}")
            elif -1800 <= diff <= 5:
                try:
                    job_id = f'plan_reminder_combined_{chat_id}_{date_str}'
                    if not scheduler.get_job(job_id):
                        send_plan_notification_combined(chat_id, date_str, user_id=user_id)
                        logger.info(f"[PLAN CHECK] Объединённое уведомление отправлено сразу для чата {chat_id} на {date_str}")
                except Exception as e:
                    logger.error(f"[PLAN CHECK] Ошибка отправки объединённого уведомления: {e}", exc_info=True)

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
                # Утреннее напоминание по планам отправляется одним объединённым уведомлением (см. send_plan_notification_combined выше).
                # Здесь только напоминание с билетами за N минут до сеанса.
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

            # Планы дома: утреннее напоминание уходит одним объединённым уведомлением (send_plan_notification_combined).

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


def check_and_send_rate_reminders():
    """Через 3 часа после времени запланированного просмотра (только фильмы, не сериалы)
    отправляет сообщение «Как вам фильм X? Оцените и посмотрите факты» с кнопками Оценить и Факты.
    Не отправляет, если фильм уже оценён хотя бы одним пользователем в чате."""
    import html as html_module

    now_utc = datetime.now(pytz.utc)
    window_start = now_utc - timedelta(hours=3, minutes=20)
    window_end = now_utc - timedelta(hours=3) + timedelta(minutes=20)

    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
    try:
        with db_lock:
            cursor_local.execute('''
                SELECT p.id AS plan_id, p.chat_id, p.film_id, COALESCE(p.custom_title, m.title, 'Фильм') AS title, m.kp_id
                FROM plans p
                JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.plan_datetime >= %s AND p.plan_datetime <= %s
                  AND p.film_id IS NOT NULL
                  AND (m.is_series = 0 OR m.is_series IS NULL)
                  AND (p.rate_reminder_sent IS NULL OR p.rate_reminder_sent = FALSE)
            ''', (window_start, window_end))
            rows = cursor_local.fetchall()
    except Exception as e:
        logger.error(f"[RATE REMINDER] Ошибка запроса планов: {e}", exc_info=True)
        return
    finally:
        try:
            cursor_local.close()
        except Exception:
            pass
        try:
            conn_local.close()
        except Exception:
            pass

    for r in rows:
        if isinstance(r, dict):
            plan_id = r.get('plan_id')
            chat_id = r.get('chat_id')
            film_id = r.get('film_id')
            title = (r.get('title') or 'Фильм').strip()
            kp_id = r.get('kp_id')
        else:
            plan_id = r[0]
            chat_id = r[1]
            film_id = r[2]
            title = (r[3] if len(r) > 3 else 'Фильм') or 'Фильм'
            title = (title or 'Фильм').strip()
            kp_id = r[4] if len(r) > 4 else None

        if not kp_id:
            _mark_rate_reminder_sent(plan_id, chat_id)
            continue

        conn_check = _scheduler_conn()
        cursor_check = conn_check.cursor()
        has_rating = False
        try:
            with db_lock:
                cursor_check.execute('''
                    SELECT 1 FROM ratings
                    WHERE chat_id = %s AND film_id = %s
                      AND (is_imported = FALSE OR is_imported IS NULL)
                    LIMIT 1
                ''', (chat_id, film_id))
                has_rating = cursor_check.fetchone() is not None
        except Exception as e:
            logger.warning(f"[RATE REMINDER] Ошибка проверки оценок plan_id={plan_id}: {e}")
        finally:
            try:
                cursor_check.close()
            except Exception:
                pass
            try:
                conn_check.close()
            except Exception:
                pass

        if has_rating:
            _mark_rate_reminder_sent(plan_id, chat_id)
            continue

        title_esc = html_module.escape(str(title)[:200])
        text = f"Как вам фильм <b>{title_esc}</b>? Оцените его и посмотрите интересные факты!"
        markup = InlineKeyboardMarkup(row_width=1)
        try:
            kp_int = int(kp_id)
            markup.row(
                InlineKeyboardButton("🤔 Факты", callback_data=f"show_facts:{kp_int}"),
                InlineKeyboardButton("💬 Оценить", callback_data=f"rate_film:{kp_int}")
            )
        except (ValueError, TypeError):
            _mark_rate_reminder_sent(plan_id, chat_id)
            continue

        try:
            bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup)
            logger.info(f"[RATE REMINDER] Отправлено напоминание об оценке для «{title[:50]}» в чат {chat_id}")
        except Exception as e:
            logger.error(f"[RATE REMINDER] Ошибка отправки в чат {chat_id}: {e}", exc_info=True)
        _mark_rate_reminder_sent(plan_id, chat_id)


def _mark_rate_reminder_sent(plan_id, chat_id):
    """Отмечает план как «напоминание об оценке отправлено»."""
    conn_up = _scheduler_conn()
    cursor_up = conn_up.cursor()
    try:
        with db_lock:
            cursor_up.execute(
                'UPDATE plans SET rate_reminder_sent = TRUE WHERE id = %s AND chat_id = %s',
                (plan_id, chat_id)
            )
            conn_up.commit()
    except Exception as e:
        logger.warning(f"[RATE REMINDER] Не удалось обновить rate_reminder_sent plan_id={plan_id}: {e}")
    finally:
        try:
            cursor_up.close()
        except Exception:
            pass
        try:
            conn_up.close()
        except Exception:
            pass


# Настройка периодического вывода статистики
# Вызовы scheduler.add_job должны быть в moviebot.py после импорта модуля



# Очистка планов

def clean_home_plans():
    """Ежедневно удаляет планы дома и в кино на вчерашний день, если по фильму нет оценок (пограничные: plan+3h > конец вчера не удаляем).
    Также удаляет все планы дома на прошедшие выходные (суббота и воскресенье) в понедельник."""
    
    now = datetime.now(plans_tz)
    today = now.date()
    yesterday = (now - timedelta(days=1)).date()
    today_weekday = today.weekday()  # 0 = Monday, 6 = Sunday

    deleted_count = 0
    
    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()

    try:
        with db_lock:
            # Если сегодня понедельник, удаляем все планы дома на прошедшие выходные (суббота и воскресенье)
            if today_weekday == 0:  # Monday
                # Находим субботу и воскресенье прошлой недели
                saturday = yesterday - timedelta(days=1)  # Вчера было воскресенье, значит суббота - позавчера
                sunday = yesterday

                cursor_local.execute('''
                    SELECT p.id, p.film_id, p.chat_id, m.title, m.link, m.kp_id
                    FROM plans p
                    JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                    WHERE p.plan_type = 'home' 
                    AND DATE((p.plan_datetime AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Moscow') IN (%s, %s)
                ''', (saturday, sunday))

                weekend_rows = cursor_local.fetchall()

                weekend_plans_by_chat = {}
                for row in weekend_rows:
                    plan_id = row.get('id') if isinstance(row, dict) else row[0]
                    film_id = row.get('film_id') if isinstance(row, dict) else row[1]
                    chat_id = row.get('chat_id') if isinstance(row, dict) else row[2]
                    title = row.get('title') if isinstance(row, dict) else row[3]
                    link = row.get('link') if isinstance(row, dict) else row[4]
                    kp_id = row.get('kp_id') if isinstance(row, dict) else (row[5] if len(row) > 5 else None)
                    
                    if chat_id not in weekend_plans_by_chat:
                        weekend_plans_by_chat[chat_id] = []
                    weekend_plans_by_chat[chat_id].append({
                        'plan_id': plan_id,
                        'film_id': film_id,
                        'title': title,
                        'link': link,
                        'kp_id': str(kp_id) if kp_id is not None else None
                    })
                
                # Удаляем планы и отправляем сообщения
                for chat_id, plans in weekend_plans_by_chat.items():
                    # Удаляем все планы для этого чата
                    for plan_info in plans:
                        cursor_local.execute('DELETE FROM plans WHERE id = %s', (plan_info['plan_id'],))
                        deleted_count += 1
                    
                    # Отправляем одно сообщение на чат с кнопками для всех фильмов
                    if bot and plans:
                        try:
                            from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                            
                            if len(plans) == 1:
                                message_text = f"📅 План на фильм <b>{plans[0]['title']}</b> удалён (выходные прошли)."
                            else:
                                message_text = f"📅 Удалены планы на {len(plans)} фильмов (выходные прошли):"
                            
                            markup = InlineKeyboardMarkup(row_width=1)
                            for plan_info in plans:
                                kp_id = plan_info.get('kp_id')
                                if kp_id:
                                    button_text = f"🎬 {plan_info['title']}"
                                    if len(button_text) > 64:
                                        button_text = button_text[:61] + "..."
                                    markup.add(InlineKeyboardButton(button_text, callback_data=f"show_film_info:{kp_id}"))
                            
                            if markup.keyboard:
                                bot.send_message(chat_id, message_text, parse_mode='HTML', reply_markup=markup)
                            else:
                                bot.send_message(chat_id, message_text, parse_mode='HTML')
                        except Exception as e:
                            logger.error(f"[CLEAN HOME PLANS] Ошибка отправки сообщения для выходных: {e}", exc_info=True)
                
                logger.info(f"Очищены планы дома на выходные: {len(weekend_rows)} планов")
            
            # Конец вчерашнего дня (МСК) в UTC — для пограничных планов: не удаляем, если напоминание об оценке могло прийти уже «сегодня»
            end_yesterday_local = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)
            end_yesterday_utc = plans_tz.localize(end_yesterday_local).astimezone(pytz.utc)

            # Планы дома и в кино на вчера (с film_id — по ним проверяем оценки)
            cursor_local.execute('''
                SELECT p.id, p.film_id, p.chat_id, p.plan_type, p.plan_datetime
                FROM plans p
                WHERE p.film_id IS NOT NULL
                  AND (p.plan_type = 'home' OR p.plan_type = 'cinema')
                  AND DATE((p.plan_datetime AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Moscow') = %s
            ''', (yesterday,))
            rows = cursor_local.fetchall()

            plans_by_chat = {}
            for row in rows:
                plan_id = row.get('id') if isinstance(row, dict) else row[0]
                film_id = row.get('film_id') if isinstance(row, dict) else row[1]
                chat_id = row.get('chat_id') if isinstance(row, dict) else row[2]
                plan_type = row.get('plan_type') if isinstance(row, dict) else row[3]
                plan_dt = row.get('plan_datetime') if isinstance(row, dict) else row[4]
                if plan_dt and hasattr(plan_dt, 'replace'):
                    if plan_dt.tzinfo is None:
                        plan_dt = pytz.utc.localize(plan_dt)
                elif plan_dt:
                    plan_dt = datetime.fromisoformat(str(plan_dt).replace('Z', '+00:00'))

                # Пограничный план: если напоминание об оценке (plan+3h) пришло бы после конца вчера — не удаляем
                if plan_dt and (plan_dt + timedelta(hours=3)) > end_yesterday_utc:
                    continue

                cursor_local.execute('SELECT COUNT(*) FROM ratings WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
                count_row = cursor_local.fetchone()
                count = count_row.get('count') if isinstance(count_row, dict) else (count_row[0] if count_row else 0)
                if count != 0:
                    continue

                cursor_local.execute('SELECT title, link, kp_id FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                movie_row = cursor_local.fetchone()
                if not movie_row:
                    continue
                title = movie_row.get('title') if isinstance(movie_row, dict) else movie_row[0]
                link = movie_row.get('link') if isinstance(movie_row, dict) else movie_row[1]
                kp_id = movie_row.get('kp_id') if isinstance(movie_row, dict) else (movie_row[2] if len(movie_row) > 2 else None)
                if chat_id not in plans_by_chat:
                    plans_by_chat[chat_id] = []
                plans_by_chat[chat_id].append({
                    'plan_id': plan_id,
                    'film_id': film_id,
                    'title': title,
                    'link': link,
                    'kp_id': str(kp_id) if kp_id is not None else None,
                    'plan_type': plan_type,
                })

            for chat_id, plans in plans_by_chat.items():
                for plan_info in plans:
                    cursor_local.execute('DELETE FROM plans WHERE id = %s', (plan_info['plan_id'],))
                    deleted_count += 1

                if bot and plans:
                    try:
                        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                        if len(plans) == 1:
                            message_text = "📅 План на фильм удалён (нет оценок за вчера)."
                        else:
                            message_text = f"📅 Удалены планы на {len(plans)} фильмов (нет оценок за вчера):"
                        message_text += "\n\n🏠 — просмотр дома\n🎥 — просмотр в кино"
                        markup = InlineKeyboardMarkup(row_width=1)
                        for plan_info in plans:
                            kp_id = plan_info.get('kp_id')
                            title_short = (plan_info.get('title') or 'Фильм')[:50]
                            icon = '🏠' if plan_info.get('plan_type') == 'home' else '🎥'
                            btn_text = f"{icon} {title_short}"
                            if len(btn_text) > 64:
                                btn_text = btn_text[:61] + "..."
                            if kp_id:
                                try:
                                    kp_int = int(kp_id)
                                    markup.add(InlineKeyboardButton(btn_text, callback_data=f"back_to_film:{kp_int}"))
                                except (ValueError, TypeError):
                                    pass
                        if markup.keyboard:
                            bot.send_message(chat_id, message_text, parse_mode='HTML', reply_markup=markup)
                        else:
                            bot.send_message(chat_id, message_text, parse_mode='HTML')
                    except Exception as e:
                        logger.error(f"[CLEAN HOME PLANS] Ошибка отправки сообщения: {e}", exc_info=True)

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

    logger.info(f"Очищены планы (дома и в кино) без оценок: {deleted_count} планов")



def clean_cinema_plans():
    """Ежедневно удаляет прошедшие планы кино (фильмы и мероприятия), которые прошли более 1 дня назад"""
    from datetime import datetime, timedelta
    import pytz
    
    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
    try:
        with db_lock:
            now_utc = datetime.now(pytz.utc)
            yesterday_utc = now_utc - timedelta(days=1)
            
            # Удаляем прошедшие планы кино (фильмы), которые прошли более 1 дня назад
            cursor_local.execute("""
                DELETE FROM plans 
                WHERE plan_type = 'cinema' 
                AND film_id IS NOT NULL 
                AND plan_datetime < %s
            """, (yesterday_utc,))
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
        logger.info(f"[CLEAN CINEMA PLANS] Очищены прошедшие планы кино: {deleted_films} фильмов, {deleted_events} мероприятий")
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

def send_series_notification(chat_id, film_id, kp_id, title, season, episode, user_id=None):
    """Отправляет уведомление о выходе новой серии и проверяет следующую дату. user_id опционален (для personal chat_id=user_id)."""
    try:
        if not bot:
            logger.error("[SERIES NOTIFICATION] bot не установлен")
            return
        
        # Проверка доступа: personal — chat_id=user_id; group — проверяем подписчиков
        should_send = False
        if chat_id > 0:
            should_send = has_series_features_access(chat_id, chat_id, film_id)
        else:
            conn_sub = _scheduler_conn()
            cur_sub = None
            try:
                with db_lock:
                    cur_sub = conn_sub.cursor()
                    cur_sub.execute('SELECT user_id FROM series_subscriptions WHERE chat_id=%s AND film_id=%s AND subscribed=TRUE', (chat_id, film_id))
                    subs = cur_sub.fetchall()
                for r in (subs or []):
                    uid = r.get('user_id') if isinstance(r, dict) else r[0]
                    if has_series_features_access(chat_id, uid, film_id):
                        should_send = True
                        break
            finally:
                if cur_sub:
                    try: cur_sub.close()
                    except: pass
                try: conn_sub.close()
                except: pass
        if not should_send:
            logger.info(f"[SERIES NOTIFICATION] Пропуск — нет доступа для chat_id={chat_id}, film_id={film_id}")
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
                    conn_tz = _scheduler_conn()
                    cursor_tz = conn_tz.cursor()
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
        conn_sub = _scheduler_conn()
        cursor_sub = conn_sub.cursor()
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
        
        if not has_series_features_access(chat_id, user_id, film_id):
            logger.info(f"[SERIES CHECK] Нет доступа для user_id={user_id}, film_id={film_id} (не в первых 3)")
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
                conn_tz = _scheduler_conn()
                cursor_tz = conn_tz.cursor()
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
                conn_title = _scheduler_conn()
                cursor_title = conn_title.cursor()
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
    
    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
    
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
    
    if not bot:
        return
    
    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
    
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
                    'recommendations': '🎯 Рекомендации',
                    'tickets': '🎫 Билеты',
                    'all': '💎 Movie Planner PRO'
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
            'recommendations': '🎯 Рекомендации',
            'tickets': '🎫 Билеты',
            'all': '💎 Movie Planner PRO'
        }
        plan_name = plan_names.get(plan_type, plan_type)
        
        # Определяем список функций для подписки (тезисно: сериалы, билеты, рекомендации)
        features_list = []
        if plan_type == 'all':
            features_list = [
                '📺 Трекер сериалов',
                '🎟 Билеты и напоминания',
                '🎯 Рекомендации'
            ]
        elif plan_type == 'notifications':
            features_list = ['📺 Уведомления о сериалах']
        elif plan_type == 'recommendations':
            features_list = ['🎯 Рекомендации']
        elif plan_type == 'tickets':
            features_list = ['🎟 Билеты']
        
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
                # Для групповых подписок — одно название 💎 Movie Planner PRO
                plan_names_full = {
                    'notifications': 'Уведомления о сериалах',
                    'recommendations': 'Рекомендации',
                    'tickets': 'Билеты',
                    'all': '💎 Movie Planner PRO'
                }
                tariff_name = plan_names_full.get(plan_type, plan_type)
                
                text = "Спасибо за покупку! 🎉\n\n"
                text += f"Ваша новая подписка: <b>{tariff_name}</b>\n\n"
                text += "Входит в подписку:\n"
                if plan_type == 'all':
                    text += "📺 Трекер сериалов — серии, сезоны, уведомления о новых сериях\n"
                    text += "🎟 Билеты и напоминания — добавление билетов, уведомления перед сеансом\n"
                    text += "🎯 Рекомендации — по базе, по Кинопоиску, импорт базы\n"
                elif plan_type == 'notifications':
                    text += "📺 Уведомления о сериалах — новые серии, настройка времени, прогресс сезонов\n"
                elif plan_type == 'recommendations':
                    text += "🎯 Рекомендации — по базе, по Кинопоиску, импорт базы\n"
                elif plan_type == 'tickets':
                    text += "🎟 Билеты — добавление билетов, напоминания перед мероприятием\n"
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
            
            # Получаем информацию о подписке для админов
            sub_user_id = sub.get('user_id')
            sub_chat_id = sub.get('chat_id')
            sub_price = sub.get('price', 0)  # Полная цена подписки (fallback)
            
            # Получаем реальную сумму последнего платежа (для upgrade — доплата)
            actual_amount = sub_price
            conn_local = _scheduler_conn()
            cursor_local = conn_local.cursor()
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
    
    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
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
    
    if not bot:
        return False
    
    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
    
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


def was_event_sent_this_week(chat_id, event_types):
    """Проверяет, было ли отправлено любое из указанных событий/уведомлений на текущей неделе (понедельник-воскресенье)"""
    
    if not bot:
        return False
    
    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
    
    try:
        now = datetime.now(PLANS_TZ)
        # Находим понедельник текущей недели
        days_since_monday = now.weekday()
        monday = (now - timedelta(days=days_since_monday)).date()
        sunday = monday + timedelta(days=6)
        
        with db_lock:
            if isinstance(event_types, str):
                event_types = [event_types]
            placeholders = ','.join(['%s'] * len(event_types))
            cursor_local.execute(f"""
                SELECT id FROM event_notifications 
                WHERE chat_id = %s 
                AND event_type IN ({placeholders})
                AND sent_date >= %s 
                AND sent_date <= %s
            """, (chat_id, *event_types, monday, sunday))
            row = cursor_local.fetchone()
            return row is not None
    except Exception as e:
        logger.error(f"[EVENT NOTIFICATIONS] Ошибка проверки событий на неделе: {e}", exc_info=True)
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
    
    if not bot:
        return
    
    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
    
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
    """Проверяет расписание на выходные (пт-сб-вс) и отправляет уведомление, если нет планов домашнего просмотра.
    ПРИОРИТЕТ 1: Выполняется только в пятницу, в базовое время уведомлений пользователя.
    Если на текущей неделе уже было уведомление (нет планов дома/кино/случайное событие), не отправляет."""
    from moviebot.database.db_operations import get_notification_settings
    
    if not bot:
        return
    
    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
    
    try:
        now = datetime.now(PLANS_TZ)
        current_weekday = now.weekday()
        
        # Проверяем только в пятницу (4 = пятница)
        if current_weekday != 4:
            return
        
        # Получаем все чаты (личные и группы) — уведомление «нет планов дома» и в личку, и в группы
        with db_lock:
            cursor_local.execute("SELECT DISTINCT chat_id FROM movies")
            chat_rows = cursor_local.fetchall()
        
        for row in chat_rows:
            chat_id = row.get('chat_id') if isinstance(row, dict) else row[0]
            
            # Проверяем, включены ли напоминания (random_events_enabled = общий флаг для этой группы уведомлений)
            if not get_random_events_enabled(chat_id):
                continue
            
            # ПРИОРИТЕТ: Проверяем, было ли уже отправлено какое-то уведомление на этой неделе
            if was_event_sent_this_week(chat_id, ['weekend_reminder', 'premiere_reminder', 'random_event']):
                logger.info(f"[WEEKEND SCHEDULE] Пропуск чата {chat_id} - уже было отправлено уведомление на этой неделе")
                continue
            
            # Проверяем, отключено ли это напоминание
            with db_lock:
                cursor_local.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'reminder_weekend_films_disabled'", (chat_id,))
                reminder_disabled_row = cursor_local.fetchone()
            if reminder_disabled_row:
                is_disabled = reminder_disabled_row.get('value') if isinstance(reminder_disabled_row, dict) else reminder_disabled_row[0]
                if is_disabled == 'true':
                    continue
            
            # Получаем базовое время уведомлений пользователя (пятница - будний день)
            notify_settings = get_notification_settings(chat_id)
            if notify_settings.get('separate_weekdays') == 'false':
                base_hour = notify_settings.get('home_weekday_hour', 19)
                base_minute = notify_settings.get('home_weekday_minute', 0)
            else:
                base_hour = notify_settings.get('home_weekday_hour', 19)
                base_minute = notify_settings.get('home_weekday_minute', 0)
            
            # Проверяем, подходит ли текущее время (допуск ±30 минут для покрытия разных настроек)
            current_minutes = now.hour * 60 + now.minute
            base_minutes = base_hour * 60 + base_minute
            if abs(current_minutes - base_minutes) > 30:
                continue
            
            # Проверяем, было ли уведомление на этой неделе
            with db_lock:
                cursor_local.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'last_weekend_reminder_date'", (chat_id,))
                last_date_row = cursor_local.fetchone()
            
            # Находим понедельник текущей недели
            days_since_monday = now.weekday()
            monday = (now - timedelta(days=days_since_monday)).date()
            
            should_send = True
            if last_date_row:
                last_date_str = last_date_row.get('value') if isinstance(last_date_row, dict) else last_date_row[0]
                try:
                    last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
                    # Если уведомление было на этой неделе (с понедельника), не отправляем
                    if last_date >= monday:
                        should_send = False
                except:
                    pass
            
            if not should_send:
                continue
            
            # Проверяем, есть ли планы на выходные (пт-сб-вс) для домашнего просмотра
            friday = now.replace(hour=0, minute=0, second=0, microsecond=0)
            sunday = now.replace(hour=23, minute=59, second=59, microsecond=0) + timedelta(days=2)
            
            # Проверяем планы домашнего просмотра на выходные
            with db_lock:
                cursor_local.execute('''
                    SELECT COUNT(*) FROM plans
                    WHERE chat_id = %s 
                    AND plan_type = 'home'
                    AND plan_datetime >= %s 
                    AND plan_datetime <= %s
                ''', (chat_id, friday, sunday))
                home_plans_count = cursor_local.fetchone()
            home_count = home_plans_count.get('count') if isinstance(home_plans_count, dict) else home_plans_count[0] if home_plans_count else 0
            
            # Если нет планов домашнего просмотра, отправляем уведомление
            if home_count == 0:
                try:
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("🎲 Найти фильм", callback_data="rand_final:go"))
                    markup.add(InlineKeyboardButton("⏰ Настройки напоминаний", callback_data="settings:notifications"))
                    markup.add(InlineKeyboardButton("❌ Отменить такие уведомления", callback_data="reminder:disable:weekend_films"))
                    
                    text = "🎬 На выходных нет запланированных фильмов для домашнего просмотра!\n\n"
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
                    with db_lock:
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
    """Проверяет, нет ли планов в кинотеатре на выходные, и отправляет напоминание с кнопками-премьерами.
    ПРИОРИТЕТ 2: Выполняется только в четверг. Если на текущей неделе уже было уведомление, не отправляет."""
    from moviebot.api.kinopoisk_api import get_premieres_for_period
    
    if not bot:
        return
    
    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
    
    try:
        now = datetime.now(PLANS_TZ)
        current_weekday = now.weekday()
        
        # Проверяем только в четверг (3 = четверг)
        if current_weekday != 3:
            return
        
        # Получаем все чаты (личные и группы) — уведомление «нет планов в кино» и в личку, и в группы
        with db_lock:
            cursor_local.execute("SELECT DISTINCT chat_id FROM movies")
            chat_rows = cursor_local.fetchall()
        
        for row in chat_rows:
            chat_id = row.get('chat_id') if isinstance(row, dict) else row[0]
            
            if not get_random_events_enabled(chat_id):
                continue
            
            # ПРИОРИТЕТ: Проверяем, было ли уже отправлено какое-то уведомление на этой неделе
            if was_event_sent_this_week(chat_id, ['weekend_reminder', 'premiere_reminder', 'random_event']):
                logger.info(f"[PREMIERE REMINDER] Пропуск чата {chat_id} - уже было отправлено уведомление на этой неделе")
                continue
            
            with db_lock:
                cursor_local.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'reminder_cinema_premieres_disabled'", (chat_id,))
                reminder_disabled_row = cursor_local.fetchone()
            if reminder_disabled_row:
                is_disabled = reminder_disabled_row.get('value') if isinstance(reminder_disabled_row, dict) else reminder_disabled_row[0]
                if is_disabled == 'true':
                    continue
            
            # Проверяем, есть ли планы в кино на выходные (пт-сб-вс)
            friday = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)  # Завтра пятница
            sunday = friday + timedelta(days=2)
            
            with db_lock:
                cursor_local.execute('''
                    SELECT COUNT(*) FROM plans
                    WHERE chat_id = %s 
                    AND plan_type = 'cinema'
                    AND plan_datetime >= %s 
                    AND plan_datetime <= %s
                ''', (chat_id, friday, sunday))
                cinema_plans_count = cursor_local.fetchone()
            cinema_count = cinema_plans_count.get('count') if isinstance(cinema_plans_count, dict) else cinema_plans_count[0] if cinema_plans_count else 0
            
            # Если нет планов в кино на выходные, отправляем уведомление
            if cinema_count == 0:
                # Проверяем, было ли уведомление на этой неделе
                days_since_monday = now.weekday()
                monday = (now - timedelta(days=days_since_monday)).date()
                
                with db_lock:
                    cursor_local.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'last_cinema_reminder_date'", (chat_id,))
                    last_reminder_row = cursor_local.fetchone()
                
                should_send = True
                if last_reminder_row:
                    last_reminder_str = last_reminder_row.get('value') if isinstance(last_reminder_row, dict) else last_reminder_row[0]
                    try:
                        last_reminder = datetime.strptime(last_reminder_str, '%Y-%m-%d').date()
                        # Если уведомление было на этой неделе (с понедельника), не отправляем
                        if last_reminder >= monday:
                            should_send = False
                    except:
                        pass
                
                if should_send:
                    try:
                        premieres = get_premieres_for_period('current_month')
                        
                        text = "🎬 На выходные (пятница, суббота, воскресенье) нет запланированных походов в кино!\n\n"
                        text += "Посмотрите, какие премьеры сейчас идут:"
                        
                        markup = InlineKeyboardMarkup(row_width=1)
                        
                        # Добавляем кнопки с несколькими премьерами (до 5)
                        if premieres:
                            for i, p in enumerate(premieres[:5], 1):
                                kp_id = p.get('kinopoiskId') or p.get('filmId')
                                title = p.get('nameRu') or p.get('nameOriginal') or 'Без названия'
                                year = p.get('year') or ''
                                
                                if kp_id:
                                    button_text = f"{i}. {title}"
                                    if year:
                                        button_text += f" ({year})"
                                    if len(button_text) > 50:
                                        button_text = button_text[:47] + "..."
                                    markup.add(InlineKeyboardButton(button_text, callback_data=f"premiere_detail:{kp_id}:current_month"))
                        
                        # Добавляем общую кнопку "Все премьеры"
                        markup.add(InlineKeyboardButton("📅 Все премьеры", callback_data="start_menu:premieres"))
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
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass

def check_and_send_random_events():
    """Проверяет и отправляет случайные события (ПРИОРИТЕТ 3).
    Работает только в пт/сб/вс, только если на неделе не было других уведомлений.
    Чередует типы событий: с выбором участника и без (игра в кубик)."""
    
    if not bot:
        return
    
    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
    
    try:
        now = datetime.now(PLANS_TZ)
        current_weekday = now.weekday()
        
        # Проверяем только в пт/сб/вс (4=пятница, 5=суббота, 6=воскресенье)
        if current_weekday not in [4, 5, 6]:
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
                    continue
            except Exception as e:
                logger.warning(f"[RANDOM EVENTS] Не удалось получить информацию о чате {chat_id}: {e}")
                continue
            
            if not get_random_events_enabled(chat_id):
                continue
            
            # ПРИОРИТЕТ: Проверяем, было ли уже отправлено какое-то уведомление на этой неделе
            if was_event_sent_this_week(chat_id, ['weekend_reminder', 'premiere_reminder', 'random_event']):
                logger.info(f"[RANDOM EVENTS] Пропуск чата {chat_id} - уже было отправлено уведомление на этой неделе")
                continue
            
            # Определяем, какой тип события отправить (чередование)
            # Проверяем, какое событие было последним
            with db_lock:
                cursor_local.execute("""
                    SELECT event_type FROM event_notifications 
                    WHERE chat_id = %s 
                    AND event_type = 'random_event'
                    ORDER BY sent_date DESC 
                    LIMIT 1
                """, (chat_id,))
                last_event_row = cursor_local.fetchone()
            
            # Определяем тип события: если последнее было с участником, отправляем кубик, и наоборот
            send_participant_event = True  # По умолчанию отправляем событие с участником
            if last_event_row:
                # Проверяем, было ли последнее событие с участником (по дате последнего выбора участника)
                with db_lock:
                    cursor_local.execute(
                        "SELECT value FROM settings WHERE chat_id = %s AND key = 'last_random_participant_date'",
                        (chat_id,)
                    )
                    last_participant_row = cursor_local.fetchone()
                    cursor_local.execute(
                        "SELECT value FROM settings WHERE chat_id = %s AND key = 'last_dice_game_date'",
                        (chat_id,)
                    )
                    last_dice_row = cursor_local.fetchone()
                
                last_participant_date = None
                last_dice_date = None
                
                if last_participant_row:
                    try:
                        last_participant_date = datetime.strptime(last_participant_row.get('value') if isinstance(last_participant_row, dict) else last_participant_row[0], '%Y-%m-%d').date()
                    except:
                        pass
                
                if last_dice_row:
                    try:
                        last_dice_date = datetime.strptime(last_dice_row.get('value') if isinstance(last_dice_row, dict) else last_dice_row[0], '%Y-%m-%d').date()
                    except:
                        pass
                
                # Если последнее было с участником, отправляем кубик, и наоборот
                if last_participant_date and last_dice_date:
                    # Сравниваем даты: если последнее было с участником (дата больше), отправляем кубик
                    send_participant_event = last_dice_date >= last_participant_date
                elif last_participant_date:
                    send_participant_event = False  # Было с участником, отправляем кубик
                elif last_dice_date:
                    send_participant_event = True  # Был кубик, отправляем с участником
                # Если нет ни одного события, send_participant_event остается True (по умолчанию)
            
            # Отправляем соответствующее событие
            if send_participant_event:
                # Отправляем событие с выбором участника (собственное соединение — без путаницы с курсором цикла)
                _send_random_participant_event(chat_id, now)
            else:
                # Отправляем событие с игрой в кубик
                from moviebot.utils.random_events import send_dice_game_event
                if send_dice_game_event(chat_id, skip_checks=False):
                    mark_event_sent(chat_id, 'random_event')
                    with db_lock:
                        cursor_local.execute('''
                            INSERT INTO settings (chat_id, key, value)
                            VALUES (%s, 'last_dice_game_date', %s)
                            ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                        ''', (chat_id, now.date().isoformat()))
                        conn_local.commit()
                    logger.info(f"[RANDOM EVENTS] Отправлено событие с кубиком для чата {chat_id}")
    except Exception as e:
        logger.error(f"[RANDOM EVENTS] Ошибка в check_and_send_random_events: {e}", exc_info=True)
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass


def _send_random_participant_event(chat_id, now):
    """Отправка события с выбором случайного участника. Собственное соединение к БД — только этот чат, без путаницы с курсором в цикле по чатам."""
    conn_own = None
    cur_own = None
    try:
        from moviebot.bot.bot_init import BOT_ID
        current_bot_id = BOT_ID
        if current_bot_id is None:
            try:
                current_bot_id = bot.get_me().id
            except Exception:
                current_bot_id = None

        conn_own = _scheduler_conn()
        cur_own = conn_own.cursor()

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
            cur_own.execute(query, params)
            participants = cur_own.fetchall()

        if not participants:
            return False

        participant = random.choice(participants)
        user_id_raw = participant.get('user_id') if isinstance(participant, dict) else participant[0]
        try:
            selected_user_id = int(user_id_raw)
        except (TypeError, ValueError):
            return False
        username = participant.get('username') if isinstance(participant, dict) else participant[1]

        if username:
            user_name = f"@{username}"
        else:
            try:
                member = bot.get_chat_member(chat_id, selected_user_id)
                user_name = member.user.first_name or "участник"
            except Exception:
                user_name = "участник"

        # Кнопка только для выбранного: callback_data = rand_final:go:{selected_user_id} — в обработчике сравниваем call.from_user.id с этим id
        callback_payload = f"rand_final:go:{selected_user_id}"
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("🎲 Найти фильм", callback_data=callback_payload))
        markup.add(InlineKeyboardButton("❌ Отменить такие уведомления", callback_data="reminder:disable:random_events"))
        markup.add(InlineKeyboardButton("❌ Закрыть", callback_data="random_event:close"))

        text = "🔮 Вас посетил дух выбора случайного фильма!\n\n"
        text += f"Он выбрал <b>{user_name}</b> для выбора фильма для вашей компании."

        bot.send_message(
            chat_id=chat_id,
            text=text,
            reply_markup=markup,
            parse_mode='HTML'
        )

        mark_event_sent(chat_id, 'random_event')

        with db_lock:
            cur_own.execute('''
                INSERT INTO settings (chat_id, key, value)
                VALUES (%s, 'last_random_participant_date', %s)
                ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
            ''', (chat_id, now.date().isoformat()))
            conn_own.commit()

        logger.info(f"[RANDOM EVENTS] Отправлено событие: чат {chat_id}, выбранный участник user_id={selected_user_id}, callback_data={callback_payload!r}")
        return True
    except Exception as e:
        logger.error(f"[RANDOM EVENTS] Ошибка при отправке события с участником: {e}", exc_info=True)
        return False
    finally:
        if cur_own:
            try:
                cur_own.close()
            except Exception:
                pass
        if conn_own:
            try:
                conn_own.close()
            except Exception:
                pass


def choose_random_participant():
    """УСТАРЕВШАЯ ФУНКЦИЯ: Используйте check_and_send_random_events вместо этого.
    Оставлена для обратной совместимости, но теперь просто вызывает новую функцию."""
    check_and_send_random_events()

def start_dice_game():
    """УСТАРЕВШАЯ ФУНКЦИЯ: Используйте check_and_send_random_events вместо этого.
    Оставлена для обратной совместимости, но теперь просто вызывает новую функцию."""
    check_and_send_random_events()


# --- Онбординг: уведомления новым пользователям (разнесены по минутам, чтобы не шли вместе) ---
EXTENSION_URL = "https://chromewebstore.google.com/detail/movie-planner-bot/fldeclcfcngcjphhklommcebkpfipdol"


def _get_first_start_per_user(cursor_local, since_hours=80):
    """Возвращает словарь user_id -> first_start (datetime) для пользователей с /start за последние since_hours часов."""
    sh = int(since_hours)
    interval_sql = "INTERVAL '%d hours'" % sh
    try:
        with db_lock:
            cursor_local.execute("""
                SELECT user_id, MIN(timestamp) as first_ts
                FROM stats
                WHERE command_or_action = '/start' AND user_id > 0
                AND timestamp >= NOW() - """ + interval_sql + """
                GROUP BY user_id
            """)
            rows = cursor_local.fetchall()
    except Exception as e:
        try:
            with db_lock:
                cursor_local.execute("""
                    SELECT user_id, MIN(timestamp::timestamptz) as first_ts
                    FROM stats
                    WHERE command_or_action = '/start' AND user_id > 0
                    AND timestamp::timestamptz >= NOW() - """ + interval_sql + """
                    GROUP BY user_id
                """)
            rows = cursor_local.fetchall()
        except Exception as e2:
            logger.warning(f"[ONBOARDING] Ошибка получения first_start: {e}, {e2}")
            return {}
    result = {}
    for r in rows:
        uid = r.get('user_id') if isinstance(r, dict) else r[0]
        ts = r.get('first_ts') if isinstance(r, dict) else r[1]
        if ts and uid:
            if hasattr(ts, 'tzinfo') and ts.tzinfo is None:
                ts = pytz.utc.localize(ts) if pytz else ts
            result[uid] = ts
    return result


def _onboarding_set_sent(chat_id, key):
    """Использует отдельное соединение, чтобы не закрывать глобальное (и не ломать курсор в цикле onboarding)."""
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from moviebot.config import DATABASE_URL
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    cur = conn.cursor()
    try:
        with db_lock:
            cur.execute("""
                INSERT INTO settings (chat_id, key, value) VALUES (%s, %s, '1')
                ON CONFLICT (chat_id, key) DO UPDATE SET value = '1'
            """, (chat_id, key))
            conn.commit()
    finally:
        try:
            cur.close()
        except:
            pass
        try:
            conn.close()
        except:
            pass


def _onboarding_was_sent(chat_id, key, cursor_local):
    with db_lock:
        cursor_local.execute("SELECT value FROM settings WHERE chat_id = %s AND key = %s", (chat_id, key))
        row = cursor_local.fetchone()
    if not row:
        return False
    val = row.get('value') if isinstance(row, dict) else row[0]
    return val == '1' or val == 'true'


def _user_has_blocked_bot(chat_id, cursor_local):
    """Проверяет, помечен ли пользователь как заблокировавший бота (403 blocked)."""
    with db_lock:
        cursor_local.execute(
            "SELECT value FROM settings WHERE chat_id = %s AND key = %s",
            (chat_id, 'bot_blocked_by_user')
        )
        row = cursor_local.fetchone()
    if not row:
        return False
    val = row.get('value') if isinstance(row, dict) else row[0]
    return str(val).lower() in ('1', 'true')


def _onboarding_mark_bot_blocked(user_id):
    """Помечает пользователя как заблокировавшего бота (отдельное соединение)."""
    import psycopg2
    from psycopg2.extras import RealDictCursor
    from moviebot.config import DATABASE_URL
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    cur = conn.cursor()
    try:
        with db_lock:
            cur.execute("""
                INSERT INTO settings (chat_id, key, value) VALUES (%s, %s, '1')
                ON CONFLICT (chat_id, key) DO UPDATE SET value = '1'
            """, (user_id, 'bot_blocked_by_user'))
            conn.commit()
        logger.info(f"[ONBOARDING] Помечен как заблокировавший бота: user_id={user_id}")
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def _is_telegram_blocked_error(exc):
    """Проверяет, что исключение — «пользователь заблокировал бота» (403)."""
    msg = (getattr(exc, 'description', None) or str(exc) or '').lower()
    code = str(getattr(exc, 'error_code', '') or '')
    return code == '403' or ('403' in code and 'blocked' in msg) or ('forbidden' in msg and 'blocked by the user' in msg)


def check_onboarding_24h():
    """Через 24ч после первого /start: если пользователь ничего не сделал (0 фильмов) — привет + запланировать + 3 подборки."""
    from moviebot.database.db_operations import get_latest_tags

    if not bot:
        return
    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
    now = datetime.now(PLANS_TZ)
    if now.tzinfo is None:
        now = pytz.utc.localize(now)
    try:
        first_starts = _get_first_start_per_user(cursor_local, since_hours=80)
        for user_id, first_ts in first_starts.items():
            if first_ts.tzinfo is None:
                first_ts = pytz.utc.localize(first_ts)
            delta = (now - first_ts).total_seconds() / 3600
            if not (23 <= delta <= 25):
                continue
            chat_id = user_id
            if _onboarding_was_sent(chat_id, 'onboarding_24h_sent', cursor_local):
                continue
            if _user_has_blocked_bot(chat_id, cursor_local):
                continue
            with db_lock:
                cursor_local.execute("SELECT COUNT(*) FROM movies WHERE chat_id = %s", (chat_id,))
                cnt = cursor_local.fetchone()
            movies_count = cnt.get('count', 0) if isinstance(cnt, dict) else (cnt[0] if cnt else 0)
            if movies_count > 0:
                continue
            text = (
                "Привет! Вижу, вы добавили Movie Planner, но пока ничего не попробовали 😅\n\n"
                "Давайте запланируем фильм на выходные? Просто пришлите в чат ссылку на любой фильм или сериал с Кинопоиска — его можно добавить в базу и запланировать просмотр. Далее нужно будет нажать \"Запланировать\", выбрать формат — \"🏠 Дома\" или \"🎥 В кино\", и своим языком указать, когда хотите посмотреть фильм: например, \"суббота вечер\". Готово!\n\n"
                "Вам придет напоминание о запланированном просмотре.\n\n"
                "Также, чтобы добавить фильмы в вашу базу, вы можете добавить одну из подборок:"
            )
            markup = InlineKeyboardMarkup(row_width=1)
            try:
                bot_username = bot.get_me().username
            except Exception:
                bot_username = None
            from moviebot.bot.handlers.tags import strip_html_tags as _strip_tag_name
            for tag in get_latest_tags(3):
                name = _strip_tag_name(tag.get('name') or '')[:40]
                short = tag.get('short_code') or ''
                if bot_username and short:
                    markup.add(InlineKeyboardButton(name, url=f"https://t.me/{bot_username}?start=tag_{short}"))
                else:
                    markup.add(InlineKeyboardButton(name, callback_data="noop"))
            try:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
                _onboarding_set_sent(chat_id, 'onboarding_24h_sent')
                logger.info(f"[ONBOARDING 24H] Отправлено user_id={user_id}")
            except Exception as e:
                if _is_telegram_blocked_error(e):
                    _onboarding_mark_bot_blocked(user_id)
                logger.warning(f"[ONBOARDING 24H] Не удалось отправить user_id={user_id}: {e}")
    except Exception as e:
        logger.error(f"[ONBOARDING 24H] Ошибка: {e}", exc_info=True)
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass


def check_onboarding_plan_reminder():
    """Через 2–3 дня после /start: если добавил хотя бы 1 фильм, но не запланировал — напомнить запланировать + кнопка к фильму + 3 подборки."""
    from moviebot.database.db_operations import get_latest_tags

    if not bot:
        return
    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
    now = datetime.now(PLANS_TZ)
    if now.tzinfo is None:
        now = pytz.utc.localize(now)
    try:
        first_starts = _get_first_start_per_user(cursor_local, since_hours=80)
        for user_id, first_ts in first_starts.items():
            if first_ts.tzinfo is None:
                first_ts = pytz.utc.localize(first_ts)
            delta_days = (now - first_ts).total_seconds() / 86400
            if not (2 <= delta_days <= 3):
                continue
            chat_id = user_id
            if _onboarding_was_sent(chat_id, 'onboarding_plan_reminder_sent', cursor_local):
                continue
            with db_lock:
                cursor_local.execute(
                    "SELECT COUNT(*) FROM movies WHERE chat_id = %s", (chat_id,)
                )
                mrow = cursor_local.fetchone()
                cursor_local.execute(
                    "SELECT COUNT(*) FROM plans WHERE chat_id = %s AND user_id = %s",
                    (chat_id, user_id)
                )
                prow = cursor_local.fetchone()
            movies_count = mrow.get('count', 0) if isinstance(mrow, dict) else (mrow[0] if mrow else 0)
            plans_count = prow.get('count', 0) if isinstance(prow, dict) else (prow[0] if prow else 0)
            if movies_count == 0 or plans_count > 0:
                continue
            with db_lock:
                cursor_local.execute(
                    "SELECT id, title, kp_id FROM movies WHERE chat_id = %s ORDER BY id DESC LIMIT 1",
                    (chat_id,)
                )
                film_row = cursor_local.fetchone()
            if not film_row:
                continue
            film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
            title = film_row.get('title') or 'фильм'
            if isinstance(title, str):
                title = title[:80]
            kp_id = film_row.get('kp_id') if isinstance(film_row, dict) else film_row[2]
            try:
                import html as html_module
                title_esc = html_module.escape(str(title)[:80])
            except Exception:
                title_esc = str(title)[:80]
            text = (
                f"Вы добавили фильм {title_esc}! 🎬\n\n"
                "Хотите запланировать просмотр? Просто перейдите к описанию фильма, выберите \"Запланировать\" под карточкой — и установите напоминание о просмотре.\n\n"
                "Также, вот несколько подборок фильмов, которые вы можете добавить, чтобы наполнить вашу базу фильмов:"
            )
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("📖 Перейти к описанию", callback_data=f"back_to_film:{kp_id or film_id}"))
            try:
                bot_username = bot.get_me().username
            except Exception:
                bot_username = None
            from moviebot.bot.handlers.tags import strip_html_tags as _strip_tag_name
            for tag in get_latest_tags(3):
                name = _strip_tag_name(tag.get('name') or '')[:40]
                short = tag.get('short_code') or ''
                if bot_username and short:
                    markup.add(InlineKeyboardButton(name, url=f"https://t.me/{bot_username}?start=tag_{short}"))
                else:
                    markup.add(InlineKeyboardButton(name, callback_data="noop"))
            try:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
                _onboarding_set_sent(chat_id, 'onboarding_plan_reminder_sent')
                logger.info(f"[ONBOARDING PLAN] Отправлено user_id={user_id}, film_id={film_id}")
            except Exception as e:
                logger.warning(f"[ONBOARDING PLAN] Не удалось отправить user_id={user_id}: {e}")
    except Exception as e:
        logger.error(f"[ONBOARDING PLAN] Ошибка: {e}", exc_info=True)
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass


def check_onboarding_48h():
    """Через 48–72ч после /start: если всё ещё нет добавленных фильмов — предложить расширение."""

    if not bot:
        return
    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
    now = datetime.now(PLANS_TZ)
    if now.tzinfo is None:
        now = pytz.utc.localize(now)
    try:
        first_starts = _get_first_start_per_user(cursor_local, since_hours=80)
        for user_id, first_ts in first_starts.items():
            if first_ts.tzinfo is None:
                first_ts = pytz.utc.localize(first_ts)
            delta_h = (now - first_ts).total_seconds() / 3600
            if not (48 <= delta_h <= 72):
                continue
            chat_id = user_id
            if _onboarding_was_sent(chat_id, 'onboarding_48h_sent', cursor_local):
                continue
            if _user_has_blocked_bot(chat_id, cursor_local):
                continue
            with db_lock:
                cursor_local.execute("SELECT COUNT(*) FROM movies WHERE chat_id = %s", (chat_id,))
                cnt = cursor_local.fetchone()
            movies_count = cnt.get('count', 0) if isinstance(cnt, dict) else (cnt[0] if cnt else 0)
            if movies_count > 0:
                continue
            text = (
                "Привет! Вижу, Вы пока не успели ничего добавить в Movie Planner 😊\n\n"
                "Вот что сильно упрощает жизнь: установите расширение для Chrome — и добавляйте фильмы/сериалы одним кликом прямо с Кинопоиска, IMDb или Letterboxd, а также любые фильмы с большинства стримингов.\n\n"
                "После установки просто зайдите на страницу любого фильма на Кинопоиске — и сможете добавить его в базу и запланировать просмотр. Попробуете? 😄"
            )
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("💻 Перейти к расширению", url=EXTENSION_URL))
            try:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
                _onboarding_set_sent(chat_id, 'onboarding_48h_sent')
                logger.info(f"[ONBOARDING 48H] Отправлено user_id={user_id}")
            except Exception as e:
                if _is_telegram_blocked_error(e):
                    _onboarding_mark_bot_blocked(user_id)
                logger.warning(f"[ONBOARDING 48H] Не удалось отправить user_id={user_id}: {e}")
    except Exception as e:
        logger.error(f"[ONBOARDING 48H] Ошибка: {e}", exc_info=True)
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass


def check_unwatched_films_notification():
    """Проверяет и отправляет уведомления о непросмотренных фильмах пользователям с более чем 5 фильмами.
    ПРИОРИТЕТ 4 (ниже остальных): Выполняется только в воскресенье или вторник, после 14:00 по местному времени.
    Примерно раз в 10 дней, не более 1 сообщения в день."""
    
    if not bot:
        return
    
    conn_local = _scheduler_conn()
    cursor_local = conn_local.cursor()
    
    try:
        now_utc = datetime.now(PLANS_TZ)
        current_weekday = now_utc.weekday()
        
        # Проверяем только в воскресенье (6) или вторник (1)
        if current_weekday not in [1, 6]:  # 1=вторник, 6=воскресенье
            return
        
        # Получаем всех уникальных пользователей из таблицы movies
        # Используем chat_id = user_id для личных чатов, либо user_id из stats для групповых
        with db_lock:
            # Получаем пользователей из личных чатов (chat_id = user_id)
            cursor_local.execute("""
                SELECT DISTINCT chat_id as user_id, chat_id as chat_id
                FROM movies
                WHERE chat_id > 0
            """)
            personal_users = cursor_local.fetchall()
            
            # Получаем пользователей из групповых чатов
            cursor_local.execute("""
                SELECT DISTINCT user_id, chat_id
                FROM stats
                WHERE user_id IS NOT NULL AND chat_id < 0
            """)
            group_users = cursor_local.fetchall()
        
        all_users = []
        for row in personal_users:
            if isinstance(row, dict):
                all_users.append((row.get('user_id'), row.get('chat_id')))
            else:
                all_users.append((row[0], row[0]))
        
        for row in group_users:
            if isinstance(row, dict):
                all_users.append((row.get('user_id'), row.get('chat_id')))
            else:
                all_users.append((row[0], row[1]))
        
        # Убираем дубликаты
        all_users = list(set(all_users))
        
        for user_id, chat_id in all_users:
            try:
                # Проверяем, что это валидный пользователь
                if not user_id or not chat_id:
                    continue
                
                # Проверяем часовой пояс пользователя
                user_tz = get_user_timezone_or_default(user_id)
                now_user = now_utc.astimezone(user_tz)
                
                # Проверяем, что время во второй половине дня (после 14:00)
                if now_user.hour < 14:
                    continue
                
                # Проверяем, не слишком поздно (до 22:00, чтобы не мешать спать)
                if now_user.hour >= 22:
                    continue
                
                # Проверяем, не отключены ли уведомления о непросмотренных фильмах
                with db_lock:
                    cursor_local.execute("""
                        SELECT value FROM settings 
                        WHERE chat_id = %s AND key = 'reminder_unwatched_films_disabled'
                    """, (chat_id,))
                    reminder_disabled_row = cursor_local.fetchone()
                if reminder_disabled_row:
                    is_disabled = reminder_disabled_row.get('value') if isinstance(reminder_disabled_row, dict) else reminder_disabled_row[0]
                    if is_disabled == 'true':
                        continue
                
                # Проверяем, не было ли сегодня других уведомлений
                today = now_utc.date()
                with db_lock:
                    cursor_local.execute("""
                        SELECT id FROM event_notifications 
                        WHERE chat_id = %s 
                        AND sent_date = %s
                    """, (chat_id, today))
                    today_notifications = cursor_local.fetchone()
                
                if today_notifications:
                    continue
                
                # Проверяем, когда последний раз отправлялось это уведомление
                with db_lock:
                    cursor_local.execute("""
                        SELECT value FROM settings 
                        WHERE chat_id = %s AND key = 'last_unwatched_films_notification_date'
                    """, (chat_id,))
                    last_date_row = cursor_local.fetchone()
                
                should_send = True
                if last_date_row:
                    last_date_str = last_date_row.get('value') if isinstance(last_date_row, dict) else last_date_row[0]
                    try:
                        last_date = datetime.strptime(last_date_str, '%Y-%m-%d').date()
                        days_since = (today - last_date).days
                        # Отправляем примерно раз в 10 дней (8-12 дней - случайный интервал)
                        if days_since < 8:
                            should_send = False
                        elif days_since > 12:
                            should_send = True
                        else:
                            # В интервале 8-12 дней отправляем с вероятностью 1/5 (20%)
                            import random
                            should_send = random.random() < 0.2
                    except:
                        pass
                
                if not should_send:
                    continue
                
                # Проверяем количество непросмотренных фильмов (watched = FALSE)
                # Работает и для личных чатов, и для групповых
                unwatched_count = 0
                conn_count = _scheduler_conn()
                cursor_count = None
                try:
                    with db_lock:
                        cursor_count = conn_count.cursor()
                        cursor_count.execute("""
                            SELECT COUNT(*) FROM movies
                            WHERE chat_id = %s AND watched = FALSE
                        """, (chat_id,))
                        count_row = cursor_count.fetchone()
                    unwatched_count = count_row.get('count') if isinstance(count_row, dict) else (count_row[0] if count_row else 0)
                finally:
                    if cursor_count:
                        try:
                            cursor_count.close()
                        except:
                            pass
                    try:
                        conn_count.close()
                    except:
                        pass
                
                # Отправляем только если более 5 непросмотренных фильмов
                if unwatched_count <= 5:
                    continue
                
                # Отправляем уведомление
                try:
                    text = "👋🏻 Привет!\n\n"
                    text += "У вас есть несколько фильмов, которые вы пока не посмотрели. Может, пора выбрать один из них?"
                    
                    # Создаем кнопки: рандом по базе и отключение уведомлений
                    welcome_markup = InlineKeyboardMarkup(row_width=1)
                    welcome_markup.add(InlineKeyboardButton("🎲 Рандом по базе", callback_data="rand_mode:database"))
                    welcome_markup.add(InlineKeyboardButton("❌ Отключить такие уведомления", callback_data="reminder:disable:unwatched_films"))
                    
                    bot.send_message(
                        chat_id,
                        text,
                        reply_markup=welcome_markup,
                        parse_mode='HTML'
                    )
                    
                    # Отмечаем событие
                    mark_event_sent(chat_id, 'unwatched_films_notification')
                    
                    # Сохраняем дату последнего уведомления
                    with db_lock:
                        cursor_local.execute('''
                            INSERT INTO settings (chat_id, key, value)
                            VALUES (%s, 'last_unwatched_films_notification_date', %s)
                            ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                        ''', (chat_id, today.isoformat()))
                        conn_local.commit()
                    
                    logger.info(f"[UNWATCHED FILMS] Отправлено уведомление пользователю {user_id} в чат {chat_id} (непросмотренных: {unwatched_count})")
                except Exception as e:
                    logger.error(f"[UNWATCHED FILMS] Ошибка отправки уведомления пользователю {user_id}: {e}", exc_info=True)
            
            except Exception as e:
                logger.error(f"[UNWATCHED FILMS] Ошибка обработки пользователя {user_id}: {e}", exc_info=True)
                continue
    
    except Exception as e:
        logger.error(f"[UNWATCHED FILMS] Ошибка в check_unwatched_films_notification: {e}", exc_info=True)
    finally:
        try:
            cursor_local.close()
        except:
            pass
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
                
                # Преобразуем datetime в строку для JSON сериализации
                if next_ep and 'date' in next_ep and isinstance(next_ep['date'], datetime):
                    next_ep_copy = next_ep.copy()
                    next_ep_copy['date'] = next_ep['date'].isoformat()
                    next_ep_json = json.dumps(next_ep_copy)
                else:
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