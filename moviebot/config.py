"""
Конфигурация бота
"""
from dotenv import load_dotenv
import os
import logging
import pytz

load_dotenv()

# Логирование уже настроено в main.py в самом начале
# Не настраиваем здесь, чтобы избежать конфликтов
logger = logging.getLogger(__name__)

# Переменные окружения
TOKEN = os.getenv('BOT_TOKEN')
KP_TOKEN = os.getenv('KP_TOKEN')  # Токен для kinopoiskapiunofficial.tech
POISKKINO_TOKEN = os.getenv('POISKKINO_TOKEN')  # Токен для poiskkino.dev (резервный API)
DATABASE_URL = os.getenv('DATABASE_URL')

# Настройки API и fallback
# PRIMARY_API: 'kinopoisk_unofficial' или 'poiskkino'
PRIMARY_API = os.getenv('PRIMARY_API', 'kinopoisk_unofficial').strip().lower()
# Включить fallback на резервный API при ошибках
FALLBACK_ENABLED = os.getenv('FALLBACK_ENABLED', 'true').strip().lower() == 'true'
# Порог последовательных ошибок для переключения на fallback (по умолчанию 20)
FALLBACK_THRESHOLD = int(os.getenv('FALLBACK_THRESHOLD', '20'))
# Время в секундах до сброса счётчика ошибок (по умолчанию 5 минут)
FALLBACK_RESET_TIMEOUT = int(os.getenv('FALLBACK_RESET_TIMEOUT', '300'))

# Логирование токена для отладки (только первые и последние символы)
if TOKEN:
    token_preview = f"{TOKEN[:10]}...{TOKEN[-10:]}" if len(TOKEN) > 20 else "***"
    logger.info(f"[CONFIG] BOT_TOKEN загружен: {token_preview}")
else:
    logger.error("[CONFIG] BOT_TOKEN НЕ ЗАГРУЖЕН! Проверьте переменные окружения.")

# Логирование настроек API
logger.info(f"[CONFIG] PRIMARY_API: {PRIMARY_API}")
logger.info(f"[CONFIG] FALLBACK_ENABLED: {FALLBACK_ENABLED}")
logger.info(f"[CONFIG] FALLBACK_THRESHOLD: {FALLBACK_THRESHOLD}")
if KP_TOKEN:
    logger.info("[CONFIG] KP_TOKEN (kinopoiskapiunofficial) загружен")
else:
    logger.warning("[CONFIG] KP_TOKEN (kinopoiskapiunofficial) НЕ ЗАГРУЖЕН!")
if POISKKINO_TOKEN:
    logger.info("[CONFIG] POISKKINO_TOKEN загружен")
else:
    if PRIMARY_API == 'poiskkino' or FALLBACK_ENABLED:
        logger.warning("[CONFIG] POISKKINO_TOKEN НЕ ЗАГРУЖЕН! Fallback на poiskkino.dev не будет работать.")

# Настройки ЮKassa (из переменных окружения)
YOOKASSA_SHOP_ID = os.getenv('YOOKASSA_SHOP_ID', '').strip() if os.getenv('YOOKASSA_SHOP_ID') else None
YOOKASSA_SECRET_KEY = os.getenv('YOOKASSA_SECRET_KEY', '').strip() if os.getenv('YOOKASSA_SECRET_KEY') else None

# Настройки nalog.ru (самозанятый)
NALOG_INN = os.getenv('NALOG_INN', '').strip() if os.getenv('NALOG_INN') else None
NALOG_PASSWORD = os.getenv('NALOG_PASSWORD', '').strip() if os.getenv('NALOG_PASSWORD') else None

# Проверка обязательных переменных для ЮKassa (если используются платежи)
if YOOKASSA_SHOP_ID and not YOOKASSA_SECRET_KEY:
    logger.warning("YOOKASSA_SHOP_ID задан, но YOOKASSA_SECRET_KEY отсутствует!")
if YOOKASSA_SECRET_KEY and not YOOKASSA_SHOP_ID:
    logger.warning("YOOKASSA_SECRET_KEY задан, но YOOKASSA_SHOP_ID отсутствует!")

# Проверка обязательных переменных
if not TOKEN:
    logger.error("BOT_TOKEN не задан! Бот не может работать.")
    raise ValueError("Добавьте BOT_TOKEN в environment variables")

if not DATABASE_URL:
    logger.error("DATABASE_URL не задан! Бот не может подключиться к БД.")
    raise ValueError("DATABASE_URL не задан! Добавьте DATABASE_URL в environment variables")

# Часовой пояс для планов
PLANS_TZ = pytz.timezone('Europe/Moscow')

# Маппинг месяцев
MONTHS_MAP = {
    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
    'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
    'янв': 1, 'фев': 2, 'мар': 3, 'апр': 4, 'май': 5, 'июн': 6,
    'июл': 7, 'авг': 8, 'сен': 9, 'сент': 9, 'окт': 10, 'ноя': 11, 'дек': 12,
    'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4, 'май': 5, 'июнь': 6,
    'июль': 7, 'август': 8, 'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
}

# Маппинг дней недели
DAYS_FULL = {
    'понедельник': 0, 'пн': 0, 'в понедельник': 0, 'на понедельник': 0,
    'вторник': 1, 'вт': 1, 'во вторник': 1, 'на вторник': 1,
    'среда': 2, 'ср': 2, 'в среду': 2, 'на среду': 2,
    'четверг': 3, 'чт': 3, 'в четверг': 3, 'на четверг': 3,
    'пятница': 4, 'пт': 4, 'в пятницу': 4, 'на пятницу': 4,
    'суббота': 5, 'сб': 5, 'в субботу': 5, 'на субботу': 5,
    'воскресенье': 6, 'вс': 6, 'в воскресенье': 6, 'на воскресенье': 6,
    'в пн': 0, 'в вт': 1, 'в ср': 2, 'в чт': 3, 'в пт': 4, 'в сб': 5, 'в вс': 6,
    'на пн': 0, 'на вт': 1, 'на ср': 2, 'на чт': 3, 'на пт': 4, 'на сб': 5, 'на вс': 6
}

DAYS_MAP = DAYS_FULL  # Для обратной совместимости

# Маппинг времени дня
TIME_OF_DAY_MAP = {
    'утро': (10, 0), 'утром': (10, 0), 'с утра': (10, 0),
    'день': (14, 0), 'днем': (14, 0), 'днём': (14, 0), 'в день': (14, 0),
    'вечер': (19, 0), 'вечером': (19, 0), 'вечер': (19, 0), 'вечером': (19, 0)
}

# Дефолтные эмодзи для просмотра
DEFAULT_WATCHED_EMOJIS = "✅👍👍🏻👍🏼👍🏽👍🏾👍🏿❤️❤️‍🔥❤️‍🩹💛🧡💚💙💜🖤🤍🤎"

