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
    
    # Регистрируем callback handlers
    from moviebot.bot.callbacks.series_callbacks import register_series_callbacks
    register_series_callbacks(bot)
    
    from moviebot.bot.callbacks.payment_callbacks import register_payment_callbacks
    register_payment_callbacks(bot)
    
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

