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
IS_PRODUCTION = os.getenv('IS_PRODUCTION', 'False').lower() == 'true'
USE_WEBHOOK = os.getenv('USE_WEBHOOK', 'false').lower() == 'true'
WEBHOOK_URL = os.getenv('WEBHOOK_URL')

# В production используем только webhook, чтобы избежать конфликта 409
if IS_PRODUCTION:
    logger.info("🚀 PRODUCTION режим: запуск только webhook (polling отключен)")
    if not WEBHOOK_URL:
        logger.error("❌ IS_PRODUCTION=True, но WEBHOOK_URL не установлен! Установите WEBHOOK_URL в Railway.")
        raise ValueError("WEBHOOK_URL required in production mode")
    
    from moviebot.web.web_app import create_web_app
    app = create_web_app(bot)
    
    # Устанавливаем webhook
    try:
        bot.set_webhook(url=f"{WEBHOOK_URL}/webhook")
        logger.info(f"✅ Webhook установлен: {WEBHOOK_URL}/webhook")
    except Exception as e:
        logger.error(f"❌ Ошибка установки webhook: {e}")
    
    # Запускаем Flask приложение
    port = int(os.getenv('PORT', 8080))
    logger.info(f"🚀 Запуск Flask приложения на порту {port} (PRODUCTION)")
    app.run(host='0.0.0.0', port=port)
elif USE_WEBHOOK and WEBHOOK_URL:
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
    
    import time
    from telebot.apihelper import ApiTelegramException
    
    # Дополнительная проверка: убеждаемся, что старый файл не запущен
    import sys
    if 'moviebot.py' in sys.modules or 'moviebot.py.OLD_DO_NOT_USE' in str(sys.modules):
        logger.error("❌ ОБНАРУЖЕН ИМПОРТ СТАРОГО ФАЙЛА! Проверьте импорты.")
    
    # Проверяем, что используется правильный entry point
    if __name__ != '__main__' and 'moviebot.main' not in sys.argv[0]:
        logger.warning(f"⚠️ Неожиданный entry point: {sys.argv[0]}")
    
    # Функция для очистки webhook и подготовки к запуску
    def prepare_for_polling():
        """Подготовка к запуску polling: очистка webhook и проверки"""
        try:
            # Агрессивная очистка webhook
            bot.remove_webhook()
            logger.info("Webhook очищен")
            time.sleep(2)
            
            # Проверяем, что webhook действительно удален
            try:
                webhook_info = bot.get_webhook_info()
                if webhook_info.url:
                    logger.warning(f"⚠️ Обнаружен активный webhook: {webhook_info.url}, удаляю...")
                    bot.remove_webhook()
                    time.sleep(3)  # Увеличиваем задержку после удаления
                    # Проверяем еще раз
                    webhook_info = bot.get_webhook_info()
                    if webhook_info.url:
                        logger.error(f"❌ Webhook не удалось удалить: {webhook_info.url}")
                    else:
                        logger.info("✅ Webhook успешно удален")
            except Exception as webhook_check_e:
                logger.warning(f"Не удалось проверить webhook: {webhook_check_e}")
        except Exception as e:
            logger.warning(f"Ошибка при подготовке к polling: {e}")
    
    # Запуск polling с обработкой ошибок
    # ВАЖНО: НЕ используем бесконечный цикл, чтобы избежать конфликтов при ошибке 409
    # Если возникает 409, это означает, что уже запущен другой экземпляр polling
    # и нужно остановить текущий процесс, а не пытаться запустить новый
    
    try:
        # Подготовка к запуску
        prepare_for_polling()
        
        logger.info("✅ Запуск polling...")
        logger.info(f"✅ Используется правильный entry point: moviebot.main")
        
        # Запускаем polling
        # none_stop=True означает, что polling будет продолжать работать даже при ошибках
        # но это НЕ означает, что он будет перезапускаться при 409
        bot.polling(none_stop=True, interval=0, timeout=20, long_polling_timeout=20)
        
        # Если polling завершился без ошибки (например, KeyboardInterrupt), выходим
        logger.info("Polling завершен")
        
    except KeyboardInterrupt:
        logger.info("Остановка бота по запросу пользователя...")
        scheduler.shutdown()
        if watchdog:
            watchdog.stop()
    except ApiTelegramException as e:
        error_code = getattr(e, 'error_code', None)
        error_str = str(e)
        
        # Проверяем, является ли это ошибкой 409 (конфликт нескольких экземпляров)
        if error_code == 409 or "409" in error_str or "Conflict" in error_str or "terminated by other getUpdates" in error_str:
            logger.error(f"❌ ОШИБКА 409: Обнаружен конфликт - запущено несколько экземпляров бота!")
            logger.error(f"   Возможные причины:")
            logger.error(f"   1. Запущен другой экземпляр бота (проверьте процессы)")
            logger.error(f"   2. Активный webhook конфликтует с polling (проверьте через get_webhook_info)")
            logger.error(f"   3. Старый процесс бота не завершился полностью")
            logger.error(f"")
            logger.error(f"   РЕШЕНИЕ:")
            logger.error(f"   - Остановите ВСЕ процессы бота")
            logger.error(f"   - Убедитесь, что webhook удален: bot.remove_webhook()")
            logger.error(f"   - Подождите 5-10 секунд")
            logger.error(f"   - Запустите бота заново")
            logger.error(f"")
            logger.error(f"   Бот завершает работу для предотвращения конфликта.")
            
            # Пытаемся остановить polling перед завершением
            try:
                bot.stop_polling()
                logger.info("Polling остановлен")
            except:
                pass
            
            scheduler.shutdown()
            if watchdog:
                watchdog.stop()
            sys.exit(1)  # Завершаем процесс, чтобы не создавать конфликт
        else:
            # Другие ошибки Telegram API - логируем и пробрасываем дальше
            logger.error(f"❌ Telegram API ошибка: {e}", exc_info=True)
            logger.error(f"   error_code={error_code}, result_json={getattr(e, 'result_json', {})}")
            scheduler.shutdown()
            if watchdog:
                watchdog.stop()
            raise
            
    except Exception as e:
        error_str = str(e)
        # Проверяем, является ли это ошибкой 409 (конфликт нескольких экземпляров)
        if "409" in error_str or "Conflict" in error_str or "terminated by other getUpdates" in error_str:
            logger.error(f"❌ ОШИБКА 409: Обнаружен конфликт - запущено несколько экземпляров бота!")
            logger.error(f"   Возможные причины:")
            logger.error(f"   1. Запущен другой экземпляр бота (проверьте процессы)")
            logger.error(f"   2. Активный webhook конфликтует с polling (проверьте через get_webhook_info)")
            logger.error(f"   3. Старый процесс бота не завершился полностью")
            logger.error(f"")
            logger.error(f"   РЕШЕНИЕ:")
            logger.error(f"   - Остановите ВСЕ процессы бота")
            logger.error(f"   - Убедитесь, что webhook удален: bot.remove_webhook()")
            logger.error(f"   - Подождите 5-10 секунд")
            logger.error(f"   - Запустите бота заново")
            logger.error(f"")
            logger.error(f"   Бот завершает работу для предотвращения конфликта.")
            
            # Пытаемся остановить polling перед завершением
            try:
                bot.stop_polling()
                logger.info("Polling остановлен")
            except:
                pass
            
            scheduler.shutdown()
            if watchdog:
                watchdog.stop()
            sys.exit(1)  # Завершаем процесс, чтобы не создавать конфликт
        else:
            # Другие ошибки - логируем и пробрасываем дальше
            logger.error(f"❌ Критическая ошибка при запуске polling: {e}", exc_info=True)
            scheduler.shutdown()
            if watchdog:
                watchdog.stop()
            raise

