"""
Модуль утилит для парсинга и форматирования
"""
import re
import logging
import pytz
from datetime import datetime
from config.settings import MONTHS_MAP, DAYS_MAP
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

logger = logging.getLogger(__name__)

def extract_kp_id_from_text(text):

    """Извлекает kp_id из текста (URL или просто число)"""

    if not text:

        return None

    

    # Пытаемся найти kp_id в URL

    match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', text)

    if match:

        return match.group(2)

    

    # Если это просто число, возвращаем его

    match = re.search(r'^(\d+)$', text.strip())

    if match:

        return match.group(1)

    

    return None



def extract_kp_user_id(text):


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



def import_kp_ratings(kp_user_id, chat_id, user_id, max_count=100):


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



def get_user_timezone_or_default(user_id):


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


def check_timezone_change(user_id, message_date_utc):

    """Проверяет, изменился ли часовой пояс пользователя.

    Возвращает True если нужно уточнить часовой пояс, False если все ок"""

    try:

        current_tz = get_user_timezone(user_id)

        if not current_tz:

            # Часовой пояс не установлен - нужно уточнить

            return True

        

        # Сохраняем время последнего сообщения для анализа

        with db_lock:

            # Получаем предыдущее время сообщения

            cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = %s", (user_id, 'prev_message_utc'))

            prev_row = cursor.fetchone()

            

            if prev_row:

                prev_utc_str = prev_row.get('value') if isinstance(prev_row, dict) else prev_row[0]

                try:

                    prev_utc = datetime.fromisoformat(prev_utc_str)

                    if prev_utc.tzinfo is None:

                        prev_utc = pytz.utc.localize(prev_utc)

                    

                    # Вычисляем разницу во времени между сообщениями

                    time_diff = message_date_utc - prev_utc

                    

                    # Если разница больше 2 часов, возможно пользователь переехал

                    # Но это не надежно, поэтому просто проверяем паттерн активности

                    # Для простоты: если часовой пояс установлен, считаем что все ок

                except:

                    pass

            

            # Обновляем предыдущее время

            cursor.execute("""

                INSERT INTO settings (chat_id, key, value) 

                VALUES (%s, %s, %s) 

                ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value

            """, (user_id, 'prev_message_utc', message_date_utc.isoformat()))

            conn.commit()

        

        return False

    except Exception as e:

        logger.error(f"Ошибка проверки изменения часового пояса: {e}", exc_info=True)

        return True  # В случае ошибки лучше уточнить



def show_timezone_selection(chat_id, user_id, prompt_text="Выберите часовой пояс:"):


def show_timezone_selection(chat_id, user_id, prompt_text="Выберите часовой пояс:"):

    """Показывает выбор часового пояса пользователю"""

    current_tz = get_user_timezone(user_id)

    current_tz_name = "Москва" if not current_tz or current_tz.zone == 'Europe/Moscow' else "Сербия"

    current_tz_display = current_tz_name if current_tz else "не установлен"

    

    # Получаем текущее время в обоих часовых поясах для отображения

    moscow_tz = pytz.timezone('Europe/Moscow')

    serbia_tz = pytz.timezone('Europe/Belgrade')

    now_utc = datetime.now(pytz.utc)

    moscow_time = now_utc.astimezone(moscow_tz).strftime('%H:%M')

    serbia_time = now_utc.astimezone(serbia_tz).strftime('%H:%M')

    

    markup = InlineKeyboardMarkup(row_width=1)

    markup.add(InlineKeyboardButton(f"🇷🇺 Москва (MSK) {moscow_time}", callback_data="timezone:Moscow"))

    markup.add(InlineKeyboardButton(f"🇷🇸 Сербия (CET) {serbia_time}", callback_data="timezone:Serbia"))

    

    bot.send_message(

        chat_id,

        f"🕐 {prompt_text}\n\n"

        f"Текущий: <b>{current_tz_display}</b>\n\n"

        f"Часовой пояс будет автоматически обновляться при путешествиях.",

        reply_markup=markup,

        parse_mode='HTML'

    )



def get_watched_reactions(chat_id):


