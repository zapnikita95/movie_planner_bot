"""
Главная точка входа приложения
Создает bot, запускает webhook/polling
"""
import logging
import sys
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)
logger = logging.getLogger(__name__)

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
    process_recurring_payments
)
from moviebot.config import PLANS_TZ

# Периодическая проверка планов и отправка пропущенных уведомлений (каждые 5 минут)
scheduler.add_job(check_and_send_plan_notifications, 'interval', minutes=5, id='check_plan_notifications')

# Проверка подписок и отправка уведомлений за день до списания (каждый день в 9:00 МСК)
scheduler.add_job(check_subscription_payments, 'cron', hour=9, minute=0, timezone=PLANS_TZ, id='check_subscription_payments')

# Обработка рекуррентных платежей (каждый день в 9:00 МСК)
if process_recurring_payments:
    scheduler.add_job(process_recurring_payments, 'cron', hour=9, minute=0, timezone=PLANS_TZ, id='process_recurring_payments')

# Добавляем задачи очистки и голосования в scheduler
scheduler.add_job(clean_home_plans, 'cron', hour=9, minute=0, timezone=PLANS_TZ, id='clean_home_plans')
scheduler.add_job(start_cinema_votes, 'cron', day_of_week='mon', hour=9, minute=0, timezone=PLANS_TZ, id='start_cinema_votes')
scheduler.add_job(resolve_cinema_votes, 'cron', day_of_week='tue', hour=9, minute=0, timezone=PLANS_TZ, id='resolve_cinema_votes')
scheduler.add_job(hourly_stats, 'interval', hours=1, id='hourly_stats')

# Регистрируем все handlers
from moviebot.bot.commands import register_all_handlers
register_all_handlers(bot)
logger.info("Все handlers зарегистрированы")

# Периодическая синхронизация команд каждый час
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

# Инициализация Watchdog для мониторинга критических компонентов
try:
    # Watchdog находится в корневой директории utils/
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

# Определяем режим запуска (webhook или polling)
USE_WEBHOOK = os.getenv('USE_WEBHOOK', 'false').lower() == 'true'
WEBHOOK_URL = os.getenv('WEBHOOK_URL')

if USE_WEBHOOK and WEBHOOK_URL:
    # Режим webhook
    from moviebot.web.web_app import create_web_app
    app = create_web_app(bot)
    
    # Устанавливаем webhook
    try:
        bot.set_webhook(url=f"{WEBHOOK_URL}/webhook")
        logger.info(f"Webhook установлен: {WEBHOOK_URL}/webhook")
    except Exception as e:
        logger.error(f"Ошибка установки webhook: {e}")
    
    # Запускаем Flask приложение
    port = int(os.getenv('PORT', 5000))
    logger.info(f"Запуск Flask приложения на порту {port}")
    app.run(host='0.0.0.0', port=port)
else:
    # Режим polling
    logger.info("Запуск бота в режиме polling...")
    
    # Проверяем, не запущен ли уже polling
    try:
        # Очищаем webhook перед запуском polling (на всякий случай)
        bot.remove_webhook()
        logger.info("Webhook очищен перед запуском polling")
    except Exception as e:
        logger.warning(f"Не удалось очистить webhook перед polling: {e}")
    
    try:
        bot.polling(none_stop=True, interval=0, timeout=20)
    except KeyboardInterrupt:
        logger.info("Остановка бота...")
        scheduler.shutdown()
        if watchdog:
            watchdog.stop()
    except Exception as e:
        logger.error(f"Ошибка при запуске polling: {e}", exc_info=True)
        scheduler.shutdown()
        if watchdog:
            watchdog.stop()
        raise

