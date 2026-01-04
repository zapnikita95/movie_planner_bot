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

# Команды бота
commands = [
    BotCommand("start", "Главное меню"),
    BotCommand("list", "Список непросмотренных фильмов"),
    BotCommand("rate", "Оценить просмотренные фильмы"),
    BotCommand("plan", "Запланировать просмотр дома или в кино"),
    BotCommand("ticket", "Работа с билетами в кино"),
    BotCommand("total", "Статистика: фильмы, жанры, режиссёры, актёры и оценки"),
    BotCommand("stats", "Детальная статистика группы и участников"),
    BotCommand("settings", "Настройки")
]

# Устанавливаем команды для групповых чатов
try:
    bot.set_my_commands(commands, scope=telebot.types.BotCommandScopeAllGroupChats())
    logger.info("Команды установлены для групповых чатов")
except Exception as e:
    logger.error(f"Ошибка при установке команд для групповых чатов: {e}")

# Устанавливаем команды для личных сообщений
try:
    bot.set_my_commands(commands, scope=telebot.types.BotCommandScopeDefault())
    logger.info("Команды установлены для личных сообщений")
except Exception as e:
    logger.error(f"Ошибка при установке команд для личных сообщений: {e}")

