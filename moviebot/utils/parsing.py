"""
Модуль утилит для парсинга и форматирования
"""
import re
import logging
import pytz
from datetime import datetime, timedelta
from moviebot.config import MONTHS_MAP, DAYS_FULL, TIME_OF_DAY_MAP, PLANS_TZ
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from moviebot.database.db_operations import get_user_timezone, get_user_timezone_or_default
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.bot.bot_init import bot

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()

def extract_kp_id_from_text(text):
    """Извлекает kp_id из текста (URL или просто число)"""
    if not text:
        return None

    # Пытаемся найти kp_id в URL (поддерживаем разные форматы)
    # kinopoisk.ru/film/123, kinopoisk.ru/series/123, www.kinopoisk.ru/film/123 и т.д.
    patterns = [
        r'(?:https?://)?(?:www\.)?kinopoisk\.ru/(?:film|series)/(\d+)',
        r'(?:https?://)?(?:www\.)?kinopoisk\.com/(?:film|series)/(\d+)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)

    # Если это просто число, возвращаем его
    match = re.search(r'^(\d+)$', text.strip())
    if match:
        return match.group(1)

    return None



def extract_kp_user_id(text):
    """Извлекает ID пользователя Кинопоиска из текста (ID или ссылка)"""

    import re

    # Пробуем извлечь из ссылки

    match = re.search(r'kinopoisk\.ru/user/(\d+)', text)

    if match:

        return match.group(1)

    # Пробуем извлечь просто число

    match = re.search(r'^(\d+)$', text.strip())

    if match:

        return match.group(1)

    return None



def parse_session_time(text, user_tz):

    """Парсит время сеанса из текста в форматах:

    - 15 января 10:30

    - 17.01 15:20

    - 10.05.2025 21:40

    - 17 января 12 12 (без двоеточия)

    Возвращает datetime в user_tz или None

    """

    text = text.strip()

    now = datetime.now(user_tz)

    

    # Формат: "15 января 10:30" или "15 января 10 30" или "17 января 15:30"

    match = re.search(r'(\d{1,2})\s+([а-яё]+)\s+(\d{1,2})[: ](\d{1,2})', text)

    if match:

        day = int(match.group(1))

        month_str = match.group(2)

        hour = int(match.group(3))

        minute = int(match.group(4))

        

        month = MONTHS_MAP.get(month_str.lower())

        if month:

            year = now.year

            try:

                dt = datetime(year, month, day, hour, minute)

                dt = user_tz.localize(dt)

                if dt < now:

                    # Если дата в прошлом, берем следующий год

                    dt = datetime(year + 1, month, day, hour, minute)

                    dt = user_tz.localize(dt)

                return dt

            except ValueError:

                return None

    

    # Формат: "17.01 15:20" или "17.01.2025 15:20"

    match = re.search(r'(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?\s+(\d{1,2})[: ](\d{1,2})', text)

    if match:

        day = int(match.group(1))

        month = int(match.group(2))

        year_str = match.group(3)

        hour = int(match.group(4))

        minute = int(match.group(5))

        

        if year_str:

            year = int(year_str)

            if year < 100:

                year += 2000

        else:

            year = now.year

        

        try:

            dt = datetime(year, month, day, hour, minute)

            dt = user_tz.localize(dt)

            if dt < now:

                # Если дата в прошлом, берем следующий год

                dt = datetime(year + 1, month, day, hour, minute)

                dt = user_tz.localize(dt)

            return dt

        except ValueError:

            return None

    

    return None



def detect_timezone_from_message(message_date_utc):

    """Пытается определить часовой пояс по времени сообщения (UTC).

    Возвращает предполагаемый часовой пояс или None если неясно.

    message_date_utc - datetime объект в UTC"""

    try:

        # Получаем текущее время в UTC

        utc_now = datetime.now(pytz.utc)

        if message_date_utc.tzinfo is None:

            # Если нет таймзоны, предполагаем UTC

            msg_utc = pytz.utc.localize(message_date_utc)

        else:

            msg_utc = message_date_utc.astimezone(pytz.utc)

        

        # Вычисляем разницу между текущим временем и временем сообщения

        # Это не очень надежно, но может дать подсказку

        # Более надежный способ - анализировать время активности пользователя

        

        # Получаем час в UTC

        utc_hour = msg_utc.hour

        

        # Предполагаем, что пользователь активен в разумное время (8-23 часа местного времени)

        # Если сообщение отправлено в 16:00 UTC, и это разумное время для активности:

        # - Москва (UTC+3): 16:00 UTC = 19:00 MSK - разумно

        # - Сербия (UTC+1): 16:00 UTC = 17:00 CET - разумно

        

        # Но это неточно, поэтому просто возвращаем None

        # Лучше спросить у пользователя

        return None

    except Exception as e:

        logger.error(f"Ошибка определения часового пояса: {e}", exc_info=True)

        return None



def check_timezone_change(user_id, message_date_utc):
    """Проверяет, изменился ли часовой пояс пользователя.

    Возвращает True если нужно уточнить часовой пояс, False если все ок
    
    ВАЖНО: Если часовой пояс уже установлен, НЕ спрашиваем его снова"""

    # ВАЖНО: Вызываем get_user_timezone ВНЕ db_lock, чтобы избежать дедлока
    # так как get_user_timezone тоже использует db_lock
    current_tz = get_user_timezone(user_id)

    if not current_tz:
        # Часовой пояс не установлен - нужно уточнить
        return True
    
    # Если часовой пояс установлен, НЕ спрашиваем его снова
    # Просто обновляем время последнего сообщения (опционально, для статистики)
    # НО не прерываем планирование
    return False



def show_timezone_selection(chat_id, user_id, prompt_text="Выберите часовой пояс:"):
    """Показывает выбор часового пояса пользователю"""

    # Используем отдельное подключение к БД внутри get_user_timezone,
    # поэтому здесь достаточно просто вызвать функцию
    current_tz = get_user_timezone(user_id)

    if not current_tz:
        current_tz_display = "не установлен"
    else:
        tz_zone = current_tz.zone
        tz_display_map = {
            'Europe/Moscow': "Москва",
            'Europe/Belgrade': "Сербия",
            'Europe/Kaliningrad': "Калининград (-1 МСК)",
            'Europe/Samara': "Самара (+1 МСК)",
            'Asia/Yekaterinburg': "Екатеринбург (+2 МСК)",
            'Asia/Omsk': "Омск (+3 МСК)",
            'Asia/Novosibirsk': "Новосибирск (+4 МСК)",
            'Asia/Irkutsk': "Иркутск (+5 МСК)",
            'Asia/Yakutsk': "Якутск (+6 МСК)",
            'Asia/Vladivostok': "Владивосток (+7 МСК)",
            'Asia/Magadan': "Магадан (+8 МСК)",
            'Asia/Kamchatka': "Петропавловск-Камчатский (+9 МСК)",
        }
        current_tz_display = tz_display_map.get(tz_zone, tz_zone)

    

    # Получаем текущее время во всех поддерживаемых часовых поясах для отображения
    now_utc = datetime.now(pytz.utc)

    tz_buttons = [
        ("🇷🇺 Москва (MSK)", "Europe/Moscow", "timezone:Moscow"),
        ("🇷🇸 Сербия (CET)", "Europe/Belgrade", "timezone:Serbia"),
        ("🇷🇺 Калининград (-1 МСК)", "Europe/Kaliningrad", "timezone:Kaliningrad"),
        ("🇷🇺 Самара (+1 МСК)", "Europe/Samara", "timezone:Samara"),
        ("🇷🇺 Екатеринбург (+2 МСК)", "Asia/Yekaterinburg", "timezone:Yekaterinburg"),
        ("🇷🇺 Омск (+3 МСК)", "Asia/Omsk", "timezone:Omsk"),
        ("🇷🇺 Новосибирск (+4 МСК)", "Asia/Novosibirsk", "timezone:Novosibirsk"),
        ("🇷🇺 Иркутск (+5 МСК)", "Asia/Irkutsk", "timezone:Irkutsk"),
        ("🇷🇺 Якутск (+6 МСК)", "Asia/Yakutsk", "timezone:Yakutsk"),
        ("🇷🇺 Владивосток (+7 МСК)", "Asia/Vladivostok", "timezone:Vladivostok"),
        ("🇷🇺 Магадан (+8 МСК)", "Asia/Magadan", "timezone:Magadan"),
        ("🇷🇺 Петропавловск-Камчатский (+9 МСК)", "Asia/Kamchatka", "timezone:Kamchatka"),
    ]

    markup = InlineKeyboardMarkup(row_width=1)

    for label, tz_code, cb in tz_buttons:
        tz = pytz.timezone(tz_code)
        local_time = now_utc.astimezone(tz).strftime('%H:%M')
        markup.add(InlineKeyboardButton(f"{label} {local_time}", callback_data=cb))

    

    bot.send_message(
        chat_id,
        f"🕐 {prompt_text}\n\n"
        f"Текущий: <b>{current_tz_display}</b>\n\n"
        f"Часовой пояс будет автоматически обновляться при путешествиях.",
        reply_markup=markup,
        parse_mode='HTML'
    )

def parse_plan_date_text(text: str, user_id: int) -> datetime | None:
    """
    Парсит текст вроде 'завтра', '15 января', 'в пятницу 20:00', '20.01 19:30'
    Возвращает datetime в часовом поясе пользователя или None
    """
    text = text.strip().lower()
    user_tz = get_user_timezone_or_default(user_id)
    now = datetime.now(user_tz)

    # Сначала пробуем parse_session_time, если она есть (у тебя в коде есть вызов)
    try:
        from moviebot.bot.handlers.plan import parse_session_time
        parsed = parse_session_time(text, user_tz)
        if parsed:
            return parsed
    except ImportError:
        pass  # если нет — идём дальше

    extracted_time = None
    
    # Сначала проверяем время дня (утро, день, вечер)
    for phrase, (hour, minute) in TIME_OF_DAY_MAP.items():
        if phrase in text:
            extracted_time = (hour, minute)
            break
    
    # Если время дня не найдено, пытаемся извлечь время из формата HH:MM
    if extracted_time is None:
        time_match = re.search(r'\b(\d{1,2}):(\d{2})\b', text)
        if time_match:
            hour = int(time_match.group(1))
            minute = int(time_match.group(2))
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                extracted_time = (hour, minute)

    plan_dt = None

    # Дни недели
    target_weekday = None
    for phrase, wd in DAYS_FULL.items():
        if phrase in text:
            target_weekday = wd
            break

    if target_weekday is not None:
        current_wd = now.weekday()
        delta = (target_weekday - current_wd + 7) % 7
        if delta == 0:
            delta = 7
        plan_date = now.date() + timedelta(days=delta)
        hour, minute = extracted_time or (19 if plan_date.weekday() < 5 else 10, 0)
        plan_dt = user_tz.localize(datetime.combine(plan_date, datetime.min.time().replace(hour=hour, minute=minute)))
        return plan_dt

    # Специальные слова
    if 'сегодня' in text:
        plan_date = now.date()
        hour, minute = extracted_time or (19 if plan_date.weekday() < 5 else 10, 0)
        plan_dt = user_tz.localize(datetime.combine(plan_date, datetime.min.time().replace(hour=hour, minute=minute)))
        return plan_dt
    if 'завтра' in text:
        plan_date = now.date() + timedelta(days=1)
        hour, minute = extracted_time or (19 if plan_date.weekday() < 5 else 10, 0)
        plan_dt = user_tz.localize(datetime.combine(plan_date, datetime.min.time().replace(hour=hour, minute=minute)))
        return plan_dt

    # Текстовый формат: "15 января"
    date_match = re.search(r'(\d{1,2})\s+([а-яё]+)', text)
    if date_match:
        day = int(date_match.group(1))
        month_str = date_match.group(2)
        month = MONTHS_MAP.get(month_str)
        if month:
            year = now.year
            try:
                candidate = datetime(year, month, day)
                if candidate.date() < now.date():
                    year += 1
                hour, minute = extracted_time or (19 if candidate.weekday() < 5 else 10, 0)
                plan_dt = user_tz.localize(datetime(year, month, day, hour, minute))
                return plan_dt
            except ValueError:
                pass

    # Числовой формат: "20.01" или "20.01 20:30"
    date_match = re.search(r'(\d{1,2})[./](\d{1,2})', text)
    if date_match:
        day = int(date_match.group(1))
        month = int(date_match.group(2))
        if 1 <= month <= 12 and 1 <= day <= 31:
            year = now.year
            try:
                candidate = datetime(year, month, day)
                if candidate.date() < now.date():
                    year += 1
                hour, minute = extracted_time or (19 if candidate.weekday() < 5 else 10, 0)
                plan_dt = user_tz.localize(datetime(year, month, day, hour, minute))
                return plan_dt
            except ValueError:
                pass

    return None
