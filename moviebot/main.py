"""
Главная точка входа приложения
Создает bot, запускает webhook/polling
"""
# КРИТИЧЕСКИ ВАЖНО: Настройка logging ДО всех импортов
import logging
import sys

# Простая настройка — работает на Railway 100%
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stdout,  # Только stdout — Railway видит
    force=True  # Принудительно перезаписываем конфигурацию
)

# Дополнительно: принудительно INFO для всех
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# Удаляем все существующие handlers
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

# Добавляем только stdout handler
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stdout_handler.setFormatter(formatter)
root_logger.addHandler(stdout_handler)

# Отключаем логирование Werkzeug (Flask) и других библиотек, которые могут перехватывать логи
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.WARNING)
werkzeug_logger.propagate = False

flask_logger = logging.getLogger('flask')
flask_logger.setLevel(logging.WARNING)
flask_logger.propagate = False

urllib3_logger = logging.getLogger('urllib3')
urllib3_logger.setLevel(logging.WARNING)
urllib3_logger.propagate = False

logger = logging.getLogger(__name__)
logger.info("=== LOGGING ПОЧИНЕН === Это сообщение должно появиться в Railway")

# Теперь загружаем переменные окружения
from dotenv import load_dotenv
load_dotenv()

# Импорты конфигурации
from moviebot.config import TOKEN
from moviebot.database.db_connection import init_database
from moviebot.bot.bot_init import setup_bot_commands, sync_commands_periodically

# Инициализация базы данных
init_database()

# Импорты для бота
from apscheduler.schedulers.background import BackgroundScheduler

# Импортируем бота из bot_init (он уже создан там)
from moviebot.bot.bot_init import bot, init_bot_id

# Получаем ID бота и инициализируем его в bot_init
BOT_ID = init_bot_id()  # Использует глобальный bot из bot_init

# Очищаем старые webhook
try:
    bot.remove_webhook()
    logger.info("Старые webhook очищены")
except Exception as e:
    logger.warning(f"Не удалось очистить webhook: {e}")

# Планировщик для уведомлений
scheduler = BackgroundScheduler()
scheduler.start()

# Экспортируем scheduler в bot_init для использования в handlers
from moviebot.bot.bot_init import set_scheduler
set_scheduler(scheduler)

# Устанавливаем экземпляр бота и scheduler в модуле scheduler
from moviebot.scheduler import set_bot_instance, set_scheduler_instance
set_bot_instance(bot)
set_scheduler_instance(scheduler)

# Настраиваем задачи планировщика
from moviebot.scheduler import (
    hourly_stats,
    check_and_send_plan_notifications,
    clean_home_plans,
    clean_cinema_plans,
    start_cinema_votes,
    resolve_cinema_votes,
    check_subscription_payments,
    process_recurring_payments,
    check_weekend_schedule,
    check_premiere_reminder,
    choose_random_participant,
    start_dice_game
)
from moviebot.config import PLANS_TZ

# Периодическая проверка планов и отправка пропущенных уведомлений (каждые 5 минут)
scheduler.add_job(check_and_send_plan_notifications, 'interval', minutes=5, id='check_plan_notifications')

# Проверка подписок и отправка уведомлений за день до списания (каждый день в 9:00 МСК)
scheduler.add_job(check_subscription_payments, 'cron', hour=9, minute=0, timezone=PLANS_TZ, id='check_subscription_payments')

# Обработка рекуррентных платежей (каждый день в 9:00 МСК и каждые 10 минут для тестовых подписок)
if process_recurring_payments:
    scheduler.add_job(process_recurring_payments, 'cron', hour=9, minute=0, timezone=PLANS_TZ, id='process_recurring_payments')
    scheduler.add_job(process_recurring_payments, 'interval', minutes=10, id='process_recurring_payments_test')

# Добавляем задачи очистки и голосования в scheduler
scheduler.add_job(clean_home_plans, 'cron', hour=9, minute=0, timezone=PLANS_TZ, id='clean_home_plans')
scheduler.add_job(start_cinema_votes, 'cron', day_of_week='mon', hour=9, minute=0, timezone=PLANS_TZ, id='start_cinema_votes')
scheduler.add_job(resolve_cinema_votes, 'cron', day_of_week='tue', hour=9, minute=0, timezone=PLANS_TZ, id='resolve_cinema_votes')
scheduler.add_job(hourly_stats, 'interval', hours=1, id='hourly_stats')

# Случайные события и уведомления
scheduler.add_job(check_weekend_schedule, 'cron', day_of_week='fri', hour=10, minute=0, timezone=PLANS_TZ, id='check_weekend_schedule')
scheduler.add_job(check_premiere_reminder, 'cron', day_of_week='fri', hour=10, minute=30, timezone=PLANS_TZ, id='check_premiere_reminder')
scheduler.add_job(choose_random_participant, 'cron', day_of_week='mon-sun', hour=12, minute=0, timezone=PLANS_TZ, id='choose_random_participant')
scheduler.add_job(start_dice_game, 'cron', day_of_week='mon-sun', hour=14, minute=0, timezone=PLANS_TZ, id='start_dice_game')

# Регистрация ВСЕХ хэндлеров
logger.info("=" * 80)
logger.info("[MAIN] ===== РЕГИСТРАЦИЯ ВСЕХ HANDLERS =====")

# Импортируем модули с callback handlers для автоматической регистрации декораторов
import moviebot.bot.callbacks.film_callbacks  # noqa: F401
import moviebot.bot.callbacks.series_callbacks  # noqa: F401
import moviebot.bot.callbacks.payment_callbacks  # noqa: F401
import moviebot.bot.callbacks.premieres_callbacks  # noqa: F401
import moviebot.bot.handlers.admin  # noqa: F401

try:
    import moviebot.bot.handlers.promo  # noqa: F401
    logger.info("✅ promo handlers импортированы")
except Exception as e:
    logger.critical(f"[MAIN] ❌ КРИТИЧЕСКАЯ ОШИБКА ПРИ ИМПОРТЕ promo.py: {e}", exc_info=True)
    sys.exit(1)

import moviebot.bot.handlers.state_handlers  # noqa: F401
import moviebot.bot.handlers.text_messages  # noqa: F401

# Регистрируем handlers команд и callbacks
from moviebot.bot.handlers.start import register_start_handlers
register_start_handlers(bot)
logger.info("✅ start handlers зарегистрированы")

from moviebot.bot.handlers.list import register_list_handlers
register_list_handlers(bot)
logger.info("✅ list handlers зарегистрированы")

from moviebot.bot.handlers.seasons import register_seasons_handlers
register_seasons_handlers(bot)
logger.info("✅ seasons handlers зарегистрированы")

from moviebot.bot.handlers.plan import register_plan_handlers
register_plan_handlers(bot)
logger.info("✅ plan handlers зарегистрированы (включая plan_type: callback)")

from moviebot.bot.handlers.payment import register_payment_handlers
register_payment_handlers(bot)
logger.info("✅ payment handlers зарегистрированы")

from moviebot.bot.handlers.series import register_series_handlers
register_series_handlers(bot)
logger.info("✅ series handlers зарегистрированы (включая search_type: callback)")

try:
    from moviebot.bot.handlers.rate import register_rate_handlers
    register_rate_handlers(bot)
    logger.info("✅ rate handlers зарегистрированы")
except Exception as e:
    logger.critical(f"[MAIN] ❌ ОШИБКА ПРИ РЕГИСТРАЦИИ rate handlers: {e}", exc_info=True)
    sys.exit(1)

from moviebot.bot.handlers.stats import register_stats_handlers
register_stats_handlers(bot)
logger.info("✅ stats handlers зарегистрированы")

# Settings (обход конфликта имен)
import importlib.util
import os
base_dir = os.path.dirname(__file__)
settings_file_path = os.path.join(base_dir, 'bot', 'handlers', 'settings.py')
spec = importlib.util.spec_from_file_location("settings_module", settings_file_path)
settings_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(settings_module)
settings_module.register_settings_handlers(bot)
logger.info("✅ settings handlers зарегистрированы")

from moviebot.bot.handlers.settings.edit import register_edit_handlers
register_edit_handlers(bot)
logger.info("✅ edit handlers зарегистрированы")

from moviebot.bot.handlers.settings.clean import register_clean_handlers
register_clean_handlers(bot)
logger.info("✅ clean handlers зарегистрированы")

from moviebot.bot.handlers.settings.join import register_join_handlers
register_join_handlers(bot)
logger.info("✅ join handlers зарегистрированы")

from moviebot.bot.handlers.shazam import register_shazam_handlers
register_shazam_handlers(bot)
logger.info("✅ shazam handlers зарегистрированы")

# Callbacks
from moviebot.bot.callbacks.film_callbacks import register_film_callbacks
register_film_callbacks(bot)
logger.info("✅ film_callbacks зарегистрированы")

from moviebot.bot.callbacks.series_callbacks import register_series_callbacks
register_series_callbacks(bot)
logger.info("✅ series_callbacks зарегистрированы")

from moviebot.bot.callbacks.payment_callbacks import register_payment_callbacks
register_payment_callbacks(bot)
logger.info("✅ payment_callbacks зарегистрированы")

from moviebot.bot.callbacks.premieres_callbacks import register_premieres_callbacks
register_premieres_callbacks(bot)
logger.info("✅ premieres_callbacks зарегистрированы")

from moviebot.bot.callbacks.random_callbacks import register_random_callbacks
register_random_callbacks(bot)
logger.info("✅ random_callbacks зарегистрированы")

from moviebot.bot.handlers.text_messages import register_text_message_handlers
register_text_message_handlers(bot)
logger.info("✅ text_messages handlers зарегистрированы")

logger.info("=" * 80)
logger.info("✅ ВСЕ ХЭНДЛЕРЫ ЗАРЕГИСТРИРОВАНЫ")
logger.info("=" * 80)

# Предзагрузка модели Whisper
try:
    logger.info("Предзагрузка модели Whisper...")
    from moviebot.services.shazam_service import get_whisper
    whisper = get_whisper()
    if whisper and whisper is not False:
        logger.info("✅ Модель Whisper предзагружена и готова к использованию")
    else:
        logger.warning("⚠️ Модель Whisper недоступна, будет использован Vosk как fallback")
except Exception as e:
    logger.warning(f"⚠️ Не удалось предзагрузить Whisper: {e}. Будет загружена при первом использовании.")

# Debug-хэндлер для settings
@bot.callback_query_handler(func=lambda call: 'settings' in call.data.lower())
def debug_settings(call):
    logger.info(f"[DEBUG SETTINGS] СРАБОТАЛ! data={call.data}, user={call.from_user.id}")
    bot.answer_callback_query(call.id, text="🔧 Settings debug OK")

logger.info("✅ Debug-хэндлер для settings зарегистрирован")

# Периодическая синхронизация команд
scheduler.add_job(
    sync_commands_periodically,
    'interval',
    hours=1,
    args=[bot],
    id='sync_bot_commands',
    replace_existing=True
)

# Устанавливаем команды бота
setup_bot_commands(bot)

# Watchdog
try:
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from utils.watchdog import get_watchdog
    watchdog = get_watchdog(check_interval=60)
    watchdog.register_scheduler(scheduler)
    from moviebot.database.db_connection import get_db_connection
    watchdog.register_database(get_db_connection())
    watchdog.register_bot(bot)
    watchdog.start()
    logger.info("[INIT] ✅ Watchdog инициализирован и запущен")
except Exception as e:
    logger.error(f"[INIT] ❌ Ошибка инициализации Watchdog: {e}", exc_info=True)
    watchdog = None

# Режим запуска
IS_PRODUCTION = os.getenv('IS_PRODUCTION', 'False').lower() == 'true'
USE_WEBHOOK = os.getenv('USE_WEBHOOK', 'false').lower() == 'true'
WEBHOOK_URL = os.getenv('WEBHOOK_URL')

logger.info("=" * 80)
logger.info("[MAIN] Проверка переменных окружения:")
logger.info(f"[MAIN] IS_PRODUCTION: {IS_PRODUCTION}")
logger.info(f"[MAIN] USE_WEBHOOK: {USE_WEBHOOK}")
logger.info(f"[MAIN] WEBHOOK_URL: '{WEBHOOK_URL}'")
logger.info(f"[MAIN] PORT: '{os.getenv('PORT', 'НЕ УСТАНОВЛЕН')}'")
logger.info(f"[MAIN] RAILWAY_PUBLIC_DOMAIN: '{os.getenv('RAILWAY_PUBLIC_DOMAIN', 'НЕ УСТАНОВЛЕН')}'")
logger.info(f"[MAIN] RAILWAY_STATIC_URL: '{os.getenv('RAILWAY_STATIC_URL', 'НЕ УСТАНОВЛЕН')}'")
logger.info("=" * 80)

if IS_PRODUCTION:
    logger.info("🚀 PRODUCTION режим: запуск только webhook (polling отключен)")
    
    if not WEBHOOK_URL or not WEBHOOK_URL.strip():
        railway_domain = os.getenv('RAILWAY_PUBLIC_DOMAIN') or os.getenv('RAILWAY_STATIC_URL')
        if railway_domain and railway_domain.strip():
            WEBHOOK_URL = f"https://{railway_domain.strip()}"
            logger.info(f"[MAIN] Fallback на RAILWAY domain: {WEBHOOK_URL}")
        else:
            logger.error("❌ WEBHOOK_URL не задан в production!")
            raise ValueError("WEBHOOK_URL required")

    from moviebot.web.web_app import create_web_app
    app = create_web_app(bot)
    
    try:
        bot.set_webhook(url=f"{WEBHOOK_URL}/webhook")
        logger.info(f"✅ Webhook установлен: {WEBHOOK_URL}/webhook")
    except Exception as e:
        logger.error(f"Ошибка установки webhook: {e}")
    
    port = int(os.getenv('PORT', 8080))
    logger.info(f"🚀 Запуск Flask на порту {port}")
    app.run(host='0.0.0.0', port=port, threaded=True)

elif USE_WEBHOOK and WEBHOOK_URL:
    logger.info("Режим webhook (не production)")
    from moviebot.web.web_app import create_web_app
    app = create_web_app(bot)
    
    try:
        bot.set_webhook(url=f"{WEBHOOK_URL}/webhook")
        logger.info(f"Webhook установлен: {WEBHOOK_URL}/webhook")
    except Exception as e:
        logger.error(f"Ошибка установки webhook: {e}")
    
    port = int(os.getenv('PORT', 5000))
    logger.info(f"Запуск Flask на порту {port}")
    app.run(host='0.0.0.0', port=port, threaded=True)

else:
    logger.warning("⚠️ Polling режим отключен в production. Для локальной разработки установите USE_WEBHOOK=true или IS_PRODUCTION=false")
    sys.exit(1)

# Полling закомментирован полностью (не нужен в production)
# Если понадобится локально — раскомментируй и запусти с IS_PRODUCTION=false