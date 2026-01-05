"""
Регистрация всех command handlers
"""
import logging

logger = logging.getLogger(__name__)


def register_all_handlers(bot):
    """Регистрирует все обработчики команд и callbacks"""
    
    # Импортируем и регистрируем handlers
    from moviebot.bot.handlers.start import register_start_handlers
    register_start_handlers(bot)
    
    from moviebot.bot.handlers.list import register_list_handlers
    register_list_handlers(bot)
    
    from moviebot.bot.handlers.seasons import register_seasons_handlers
    register_seasons_handlers(bot)
    
    from moviebot.bot.handlers.plan import register_plan_handlers
    register_plan_handlers(bot)
    
    from moviebot.bot.handlers.payment import register_payment_handlers
    register_payment_handlers(bot)
    
    from moviebot.bot.handlers.series import register_series_handlers
    register_series_handlers(bot)
    
    from moviebot.bot.handlers.rate import register_rate_handlers
    register_rate_handlers(bot)
    
    from moviebot.bot.handlers.stats import register_stats_handlers
    register_stats_handlers(bot)
    
    from moviebot.bot.handlers.edit import register_edit_handlers
    register_edit_handlers(bot)
    
    from moviebot.bot.handlers.clean import register_clean_handlers
    register_clean_handlers(bot)
    
    from moviebot.bot.handlers.join import register_join_handlers
    register_join_handlers(bot)
    
    # Регистрируем callback handlers
    from moviebot.bot.callbacks.series_callbacks import register_series_callbacks
    register_series_callbacks(bot)
    
    from moviebot.bot.callbacks.payment_callbacks import register_payment_callbacks
    register_payment_callbacks(bot)
    
    from moviebot.bot.callbacks.premieres_callbacks import register_premieres_callbacks
    register_premieres_callbacks(bot)
    
    # КРИТИЧЕСКИ ВАЖНО: Импортируем модуль text_messages ДО вызова register_text_message_handlers
    # чтобы декораторы @bot_instance.message_handler выполнились при импорте
    import moviebot.bot.handlers.text_messages  # noqa: F401 - импорт для регистрации декораторов
    
    # КРИТИЧЕСКИ ВАЖНО: Импортируем модуль text_messages ДО вызова register_text_message_handlers
    # чтобы декораторы @bot_instance.message_handler выполнились при импорте
    logger.info("Импортирую модуль text_messages для регистрации декораторов...")
    import moviebot.bot.handlers.text_messages  # noqa: F401 - импорт для регистрации декораторов
    logger.info("✅ Модуль text_messages импортирован, декораторы зарегистрированы")
    
    # Регистрируем главный обработчик текстовых сообщений
    from moviebot.bot.handlers.text_messages import register_text_message_handlers
    register_text_message_handlers(bot)
    logger.info("✅ Обработчики текстовых сообщений зарегистрированы")
    
    # TODO: Раскомментировать по мере реализации handlers:
    # from moviebot.bot.handlers.seasons import register_seasons_handlers
    # register_seasons_handlers(bot)
    # 
    # from moviebot.bot.handlers.plan import register_plan_handlers
    # register_plan_handlers(bot)
    # 
    # from moviebot.bot.handlers.payment import register_payment_handlers
    # register_payment_handlers(bot)
    # 
    # from moviebot.bot.handlers.series import register_series_handlers
    # register_series_handlers(bot)
    # 
    # from moviebot.bot.handlers.rate import register_rate_handlers
    # register_rate_handlers(bot)
    # 
    # from moviebot.bot.handlers.stats import register_stats_handlers
    # register_stats_handlers(bot)
    
    # TODO: Добавить остальные handlers:
    # from moviebot.bot.handlers.seasons import register_seasons_handlers
    # register_seasons_handlers(bot)
    # 
    # from moviebot.bot.handlers.plan import register_plan_handlers
    # register_plan_handlers(bot)
    # 
    # from moviebot.bot.handlers.payment import register_payment_handlers
    # register_payment_handlers(bot)
    # 
    # from moviebot.bot.handlers.series import register_series_handlers
    # register_series_handlers(bot)
    # 
    # from moviebot.bot.handlers.rate import register_rate_handlers
    # register_rate_handlers(bot)
    # 
    # from moviebot.bot.handlers.stats import register_stats_handlers
    # register_stats_handlers(bot)
    
    logger.info("Все handlers зарегистрированы")


# Для обратной совместимости
def register_handlers(bot):
    """Алиас для register_all_handlers"""
    register_all_handlers(bot)

