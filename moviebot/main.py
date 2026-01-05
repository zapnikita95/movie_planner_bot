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
werkzeug_logger.propagate = False  # Не передаем логи Werkzeug в root logger

flask_logger = logging.getLogger('flask')
flask_logger.setLevel(logging.WARNING)
flask_logger.propagate = False

# Отключаем логирование urllib3 и других HTTP библиотек
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

# Регистрация ВСЕХ хэндлеров (явно, в одном месте)
logger.info("=" * 80)
logger.info("[MAIN] ===== РЕГИСТРАЦИЯ ВСЕХ HANDLERS =====")
bot_instance = bot  # Используем bot из bot_init

# Импортируем модули с callback handlers для автоматической регистрации декораторов
import moviebot.bot.callbacks.film_callbacks  # noqa: F401
import moviebot.bot.callbacks.series_callbacks  # noqa: F401
import moviebot.bot.callbacks.payment_callbacks  # noqa: F401
import moviebot.bot.callbacks.premieres_callbacks  # noqa: F401
import moviebot.bot.handlers.admin  # noqa: F401
import moviebot.bot.handlers.promo  # noqa: F401
import moviebot.bot.handlers.text_messages  # noqa: F401 - критично для регистрации декораторов

# Регистрируем handlers команд и callbacks
from moviebot.bot.handlers.start import register_start_handlers
register_start_handlers(bot_instance)
logger.info("✅ start handlers зарегистрированы")

from moviebot.bot.handlers.list import register_list_handlers
register_list_handlers(bot_instance)
logger.info("✅ list handlers зарегистрированы")

from moviebot.bot.handlers.seasons import register_seasons_handlers
register_seasons_handlers(bot_instance)
logger.info("✅ seasons handlers зарегистрированы")

from moviebot.bot.handlers.plan import register_plan_handlers
register_plan_handlers(bot_instance)
logger.info("✅ plan handlers зарегистрированы (включая plan_type: callback)")

from moviebot.bot.handlers.payment import register_payment_handlers
register_payment_handlers(bot_instance)
logger.info("✅ payment handlers зарегистрированы")

from moviebot.bot.handlers.series import register_series_handlers
register_series_handlers(bot_instance)
logger.info("✅ series handlers зарегистрированы (включая search_type: callback)")

from moviebot.bot.handlers.rate import register_rate_handlers
register_rate_handlers(bot_instance)
logger.info("✅ rate handlers зарегистрированы")

from moviebot.bot.handlers.stats import register_stats_handlers
register_stats_handlers(bot_instance)
logger.info("✅ stats handlers зарегистрированы")

from moviebot.bot.handlers.edit import register_edit_handlers
register_edit_handlers(bot_instance)
logger.info("✅ edit handlers зарегистрированы")

from moviebot.bot.handlers.clean import register_clean_handlers
register_clean_handlers(bot_instance)
logger.info("✅ clean handlers зарегистрированы")

from moviebot.bot.handlers.join import register_join_handlers
register_join_handlers(bot_instance)
logger.info("✅ join handlers зарегистрированы")

# Регистрируем callback handlers
from moviebot.bot.callbacks.film_callbacks import register_film_callbacks
register_film_callbacks(bot_instance)
logger.info("✅ film_callbacks зарегистрированы")

from moviebot.bot.callbacks.series_callbacks import register_series_callbacks
register_series_callbacks(bot_instance)
logger.info("✅ series_callbacks зарегистрированы")

from moviebot.bot.callbacks.payment_callbacks import register_payment_callbacks
register_payment_callbacks(bot_instance)
logger.info("✅ payment_callbacks зарегистрированы")

from moviebot.bot.callbacks.premieres_callbacks import register_premieres_callbacks
register_premieres_callbacks(bot_instance)
logger.info("✅ premieres_callbacks зарегистрированы")

# Регистрируем главный обработчик текстовых сообщений
from moviebot.bot.handlers.text_messages import register_text_message_handlers
register_text_message_handlers(bot_instance)
logger.info("✅ text_messages handlers зарегистрированы")

logger.info("=" * 80)
logger.info("✅ ВСЕ ХЭНДЛЕРЫ ЗАРЕГИСТРИРОВАНЫ")
logger.info("=" * 80)

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

# Детальное логирование переменных окружения для диагностики
logger.info("=" * 80)
logger.info("[MAIN] Проверка переменных окружения:")
logger.info(f"[MAIN] IS_PRODUCTION: {IS_PRODUCTION} (значение: '{os.getenv('IS_PRODUCTION', 'НЕ УСТАНОВЛЕН')}')")
logger.info(f"[MAIN] USE_WEBHOOK: {USE_WEBHOOK} (значение: '{os.getenv('USE_WEBHOOK', 'НЕ УСТАНОВЛЕН')}')")
logger.info(f"[MAIN] WEBHOOK_URL: '{WEBHOOK_URL}' (тип: {type(WEBHOOK_URL).__name__})")
logger.info(f"[MAIN] PORT: '{os.getenv('PORT', 'НЕ УСТАНОВЛЕН')}'")
logger.info(f"[MAIN] RAILWAY_PUBLIC_DOMAIN: '{os.getenv('RAILWAY_PUBLIC_DOMAIN', 'НЕ УСТАНОВЛЕН')}'")
logger.info(f"[MAIN] RAILWAY_STATIC_URL: '{os.getenv('RAILWAY_STATIC_URL', 'НЕ УСТАНОВЛЕН')}'")
logger.info("=" * 80)

# В production используем только webhook, чтобы избежать конфликта 409
if IS_PRODUCTION:
    logger.info("🚀 PRODUCTION режим: запуск только webhook (polling отключен)")
    
    # Проверяем WEBHOOK_URL (может быть None или пустая строка)
    if not WEBHOOK_URL or not WEBHOOK_URL.strip():
        # Пробуем использовать RAILWAY_PUBLIC_DOMAIN как fallback
        railway_domain = os.getenv('RAILWAY_PUBLIC_DOMAIN')
        if railway_domain and railway_domain.strip():
            WEBHOOK_URL = f"https://{railway_domain.strip()}"
            logger.info(f"[MAIN] Используется RAILWAY_PUBLIC_DOMAIN: {WEBHOOK_URL}")
        else:
            logger.error("❌ IS_PRODUCTION=True, но WEBHOOK_URL не установлен!")
            logger.error("   Установите в Railway одну из переменных:")
            logger.error("   - WEBHOOK_URL=https://your-domain.com")
            logger.error("   - RAILWAY_PUBLIC_DOMAIN=your-domain.railway.app (будет использован как https://your-domain.railway.app)")
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
    
    # Запуск polling с автоматическим перезапуском при ошибке 409
    # ВАЖНО: При ошибке 409 нужно правильно остановить старый polling перед запуском нового
    # чтобы избежать конфликта нескольких экземпляров
    
    max_retries = 5  # Максимальное количество попыток перезапуска
    retry_count = 0
    base_delay = 5  # Базовая задержка между попытками (секунды)
    
    while retry_count < max_retries:
        try:
            # Подготовка к запуску
            prepare_for_polling()
            
            logger.info(f"✅ Запуск polling (попытка {retry_count + 1}/{max_retries})...")
            logger.info(f"✅ Используется правильный entry point: moviebot.main")
            
            # Запускаем polling
            # none_stop=True означает, что polling будет продолжать работать даже при ошибках
            bot.polling(none_stop=True, interval=0, timeout=20, long_polling_timeout=20)
            
            # Если polling завершился без ошибки (например, KeyboardInterrupt), выходим
            logger.info("Polling завершен нормально")
            break
            
        except KeyboardInterrupt:
            logger.info("Остановка бота по запросу пользователя...")
            try:
                bot.stop_polling()
            except:
                pass
            scheduler.shutdown()
            if watchdog:
                watchdog.stop()
            break
            
        except ApiTelegramException as e:
            error_code = getattr(e, 'error_code', None)
            error_str = str(e)
            
            # Проверяем, является ли это ошибкой 409 (конфликт нескольких экземпляров)
            if error_code == 409 or "409" in error_str or "Conflict" in error_str or "terminated by other getUpdates" in error_str:
                retry_count += 1
                logger.error(f"❌ ОШИБКА 409 (попытка {retry_count}/{max_retries}): Обнаружен конфликт!")
                logger.error(f"   Возможные причины:")
                logger.error(f"   1. Активный webhook конфликтует с polling")
                logger.error(f"   2. Старый процесс polling не завершился полностью")
                logger.error(f"   3. Другой экземпляр бота запущен")
                
                # КРИТИЧЕСКИ ВАЖНО: Останавливаем текущий polling перед повторной попыткой
                try:
                    logger.info("Останавливаю текущий polling...")
                    bot.stop_polling()
                    logger.info("Polling остановлен")
                except Exception as stop_e:
                    logger.warning(f"Не удалось остановить polling явно: {stop_e}")
                
                # Увеличиваем задержку с каждой попыткой
                delay = base_delay * retry_count
                logger.info(f"⏳ Ожидание {delay} секунд перед повторной попыткой (для полной остановки старого polling)...")
                time.sleep(delay)
                
                # Агрессивная очистка webhook перед повторной попыткой
                try:
                    bot.remove_webhook()
                    time.sleep(2)
                    webhook_info = bot.get_webhook_info()
                    if webhook_info.url:
                        logger.warning(f"⚠️ Webhook все еще активен: {webhook_info.url}, удаляю еще раз...")
                        bot.remove_webhook()
                        time.sleep(3)
                except Exception as webhook_e:
                    logger.warning(f"Ошибка при очистке webhook: {webhook_e}")
                
                if retry_count >= max_retries:
                    logger.error(f"❌ Достигнуто максимальное количество попыток ({max_retries}). Бот завершает работу.")
                    scheduler.shutdown()
                    if watchdog:
                        watchdog.stop()
                    sys.exit(1)
                else:
                    logger.info(f"🔄 Повторная попытка запуска polling...")
                    continue  # Продолжаем цикл
            else:
                # Другие ошибки Telegram API - логируем и пробрасываем дальше
                logger.error(f"❌ Telegram API ошибка: {e}", exc_info=True)
                logger.error(f"   error_code={error_code}, result_json={getattr(e, 'result_json', {})}")
                try:
                    bot.stop_polling()
                except:
                    pass
                scheduler.shutdown()
                if watchdog:
                    watchdog.stop()
                raise
                
        except Exception as e:
            error_str = str(e)
            # Проверяем, является ли это ошибкой 409 (конфликт нескольких экземпляров)
            if "409" in error_str or "Conflict" in error_str or "terminated by other getUpdates" in error_str:
                retry_count += 1
                logger.error(f"❌ ОШИБКА 409 (попытка {retry_count}/{max_retries}): Обнаружен конфликт!")
                logger.error(f"   Возможные причины:")
                logger.error(f"   1. Активный webhook конфликтует с polling")
                logger.error(f"   2. Старый процесс polling не завершился полностью")
                logger.error(f"   3. Другой экземпляр бота запущен")
                
                # КРИТИЧЕСКИ ВАЖНО: Останавливаем текущий polling перед повторной попыткой
                try:
                    logger.info("Останавливаю текущий polling...")
                    bot.stop_polling()
                    logger.info("Polling остановлен")
                except Exception as stop_e:
                    logger.warning(f"Не удалось остановить polling явно: {stop_e}")
                
                # Увеличиваем задержку с каждой попыткой
                delay = base_delay * retry_count
                logger.info(f"⏳ Ожидание {delay} секунд перед повторной попыткой (для полной остановки старого polling)...")
                time.sleep(delay)
                
                # Агрессивная очистка webhook перед повторной попыткой
                try:
                    bot.remove_webhook()
                    time.sleep(2)
                    webhook_info = bot.get_webhook_info()
                    if webhook_info.url:
                        logger.warning(f"⚠️ Webhook все еще активен: {webhook_info.url}, удаляю еще раз...")
                        bot.remove_webhook()
                        time.sleep(3)
                except Exception as webhook_e:
                    logger.warning(f"Ошибка при очистке webhook: {webhook_e}")
                
                if retry_count >= max_retries:
                    logger.error(f"❌ Достигнуто максимальное количество попыток ({max_retries}). Бот завершает работу.")
                    scheduler.shutdown()
                    if watchdog:
                        watchdog.stop()
                    sys.exit(1)
                else:
                    logger.info(f"🔄 Повторная попытка запуска polling...")
                    continue  # Продолжаем цикл
            else:
                # Другие ошибки - логируем и пробрасываем дальше
                logger.error(f"❌ Критическая ошибка при запуске polling: {e}", exc_info=True)
                try:
                    bot.stop_polling()
                except:
                    pass
                scheduler.shutdown()
                if watchdog:
                    watchdog.stop()
                raise

