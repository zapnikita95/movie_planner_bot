"""
Модуль для инициализации бота
"""
import telebot
from telebot.types import BotCommand
import logging
from moviebot.config import TOKEN

logger = logging.getLogger(__name__)

# Создаем экземпляр бота
bot = telebot.TeleBot(TOKEN)

# Scheduler будет установлен при инициализации в main.py
scheduler = None

def set_scheduler(scheduler_instance):
    """Устанавливает глобальный scheduler"""
    global scheduler
    scheduler = scheduler_instance

# BOT_ID будет установлен при инициализации бота
BOT_ID = None

def init_bot_id(bot_instance=None):
    """Инициализирует BOT_ID из bot.get_me()"""
    global BOT_ID
    bot_to_use = bot_instance if bot_instance is not None else bot
    try:
        bot_info = bot_to_use.get_me()
        BOT_ID = bot_info.id
        logger.info(f"[BOT INIT] ID бота: {BOT_ID}")
    except Exception as e:
        logger.warning(f"[BOT INIT] Не удалось получить ID бота: {e}")
        BOT_ID = None
    return BOT_ID

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
    bot_to_use = bot_instance
    
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

def sync_commands_periodically(bot_instance):
    """
    Периодическая синхронизация команд (вызывается планировщиком).
    Гарантирует, что команды всегда актуальны в Telegram API.
    """
    logger.info("Периодическая синхронизация команд...")
    setup_bot_commands(bot_instance)

