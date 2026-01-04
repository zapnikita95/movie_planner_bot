"""
Модуль для инициализации бота
"""
from dotenv import load_dotenv
load_dotenv()

import telebot
from telebot.types import BotCommand
import os
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from config.settings import TOKEN
from database.db_connection import init_database

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Инициализация базы данных
init_database()

# Создание бота
bot = telebot.TeleBot(TOKEN)

# Получаем ID бота
try:
    bot_info = bot.get_me()
    BOT_ID = bot_info.id
    logger.info(f"ID бота: {BOT_ID}")
except Exception as e:
    logger.warning(f"Не удалось получить ID бота: {e}")
    BOT_ID = None

# Очищаем старые webhook
try:
    bot.remove_webhook()
    logger.info("Старые webhook очищены")
except Exception as e:
    logger.warning(f"Не удалось очистить webhook: {e}")

# Планировщик для уведомлений
scheduler = BackgroundScheduler()
scheduler.start()

# Финальный список команд для отображения в личке и группе
BOT_COMMANDS = [
    BotCommand("start", "Главное меню"),
    BotCommand("list", "Список непросмотренных фильмов"),
    BotCommand("rate", "Оценить просмотренные фильмы"),
    BotCommand("plan", "Запланировать просмотр дома или в кино"),
    BotCommand("ticket", "Работа с билетами в кино"),
    BotCommand("total", "Статистика: фильмы, жанры, режиссёры, актёры и оценки"),
    BotCommand("stats", "Детальная статистика группы и участников"),
    BotCommand("settings", "Настройки")
]

def setup_bot_commands(bot_instance=None):
    """
    Устанавливает команды бота для всех scope (личные чаты и группы).
    Эта функция должна вызываться при старте бота и периодически для синхронизации.
    
    Args:
        bot_instance: Экземпляр бота. Если None, используется глобальный bot.
    """
    bot_to_use = bot_instance if bot_instance is not None else bot
    
    # Определяем все scope для установки команд
    scopes = [
        (telebot.types.BotCommandScopeAllGroupChats(), "всех групповых чатов"),
        (telebot.types.BotCommandScopeAllChatAdministrators(), "администраторов групп"),
        (telebot.types.BotCommandScopeAllPrivateChats(), "всех личных чатов"),
        (telebot.types.BotCommandScopeDefault(), "дефолтного scope")
    ]
    
    success_count = 0
    error_count = 0
    
    for scope, description in scopes:
        try:
            bot_to_use.set_my_commands(BOT_COMMANDS, scope=scope)
            logger.info(f"✓ Команды установлены для {description}")
            success_count += 1
        except Exception as e:
            logger.error(f"✗ Ошибка при установке команд для {description}: {e}")
            error_count += 1
    
    if success_count > 0:
        logger.info(f"Команды успешно синхронизированы: {success_count}/{len(scopes)} scope")
    if error_count > 0:
        logger.warning(f"Ошибки при синхронизации команд: {error_count}/{len(scopes)} scope")
    
    return success_count == len(scopes)

def sync_commands_periodically():
    """
    Периодическая синхронизация команд (вызывается планировщиком).
    Гарантирует, что команды всегда актуальны в Telegram API.
    """
    logger.info("Периодическая синхронизация команд...")
    setup_bot_commands()

# Устанавливаем команды при импорте модуля (при старте бота)
setup_bot_commands()

# Добавляем периодическую синхронизацию команд каждый час
# Это гарантирует, что команды всегда будут актуальными, даже если Telegram API их сбросит
try:
    scheduler.add_job(
        sync_commands_periodically,
        'interval',
        hours=1,
        id='sync_bot_commands',
        replace_existing=True
    )
    logger.info("Периодическая синхронизация команд настроена (каждый час)")
except Exception as e:
    logger.warning(f"Не удалось настроить периодическую синхронизацию команд: {e}")

