import logging

from moviebot.bot.handlers.settings import settings_command

logger = logging.getLogger(__name__)


def register_settings_handlers(bot_param):
    """Регистрирует обработчики команды /settings через отдельный модуль.

    ВАЖНО: сами callback-хэндлеры для settings уже регистрируются в
    moviebot.bot.handlers.settings на глобальном bot.
    Здесь мы только вешаем команду /settings на текущий экземпляр bot_param.
    """
    logger.info("[SETTINGS_HANDLER] Регистрация обработчиков команды /settings")

    @bot_param.message_handler(commands=['settings'])
    def _settings_command_handler(message):
        """Обертка для регистрации команды /settings"""
        try:
            user_id = message.from_user.id if message.from_user else None
            chat_id = message.chat.id if message.chat else None
            logger.info(
                f"[SETTINGS_HANDLER] /settings handler вызван: "
                f"user_id={user_id}, chat_id={chat_id}"
            )
        except Exception:
            # Логируем, но не падаем, чтобы всегда дойти до settings_command
            logger.warning("[SETTINGS_HANDLER] Не удалось залогировать user_id/chat_id для /settings")

        settings_command(message)

    logger.info("[SETTINGS_HANDLER] ✅ Обработчики команды /settings зарегистрированы")

