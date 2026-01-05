"""
Регистрация всех command handlers
"""
import logging

logger = logging.getLogger(__name__)


def register_all_handlers(bot):
    """Регистрирует все обработчики команд и callbacks"""
    logger.info("=" * 80)
    logger.info("[REGISTER ALL HANDLERS] ===== НАЧАЛО РЕГИСТРАЦИИ ВСЕХ HANDLERS =====")
    
    # КРИТИЧЕСКИ ВАЖНО: Импортируем модули с декораторами ДО вызова функций регистрации
    # Это необходимо для того, чтобы декораторы @bot_instance.callback_query_handler сработали
    import moviebot.bot.handlers.series  # noqa: F401 - импорт для регистрации декораторов (search_type_callback)
    
    # Импортируем и регистрируем handlers
    logger.info("[REGISTER ALL HANDLERS] Регистрация handlers команд...")
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
    
    from moviebot.bot.handlers.rate import register_rate_handlers
    register_rate_handlers(bot)
    logger.info("✅ rate handlers зарегистрированы")
    
    from moviebot.bot.handlers.stats import register_stats_handlers
    register_stats_handlers(bot)
    logger.info("✅ stats handlers зарегистрированы")
    
    from moviebot.bot.handlers.edit import register_edit_handlers
    register_edit_handlers(bot)
    logger.info("✅ edit handlers зарегистрированы")
    
    from moviebot.bot.handlers.clean import register_clean_handlers
    register_clean_handlers(bot)
    logger.info("✅ clean handlers зарегистрированы")
    
    from moviebot.bot.handlers.join import register_join_handlers
    register_join_handlers(bot)
    logger.info("✅ join handlers зарегистрированы")
    
    # Регистрируем callback handlers
    # КРИТИЧЕСКИ ВАЖНО: Импортируем модули с callback handlers для автоматической регистрации декораторов
    
    # Callback handlers для карточки фильма (add_to_database, plan_from_added, show_facts)
    import moviebot.bot.callbacks.film_callbacks  # noqa: F401 - импорт для регистрации декораторов
    from moviebot.bot.callbacks.film_callbacks import register_film_callbacks
    register_film_callbacks(bot)  # Пустая функция, но импорт модуля уже зарегистрировал handlers
    logger.info("✅ Callback handlers для карточки фильма зарегистрированы")
    
    import moviebot.bot.callbacks.series_callbacks  # noqa: F401 - импорт для регистрации декораторов
    from moviebot.bot.callbacks.series_callbacks import register_series_callbacks
    register_series_callbacks(bot)
    logger.info("✅ Callback handlers для сериалов зарегистрированы")
    
    import moviebot.bot.callbacks.payment_callbacks  # noqa: F401 - импорт для регистрации декораторов
    from moviebot.bot.callbacks.payment_callbacks import register_payment_callbacks
    register_payment_callbacks(bot)
    logger.info("✅ Callback handlers для платежей зарегистрированы")
    
    # premieres_callbacks использует декораторы напрямую, поэтому импортируем модуль для регистрации
    import moviebot.bot.callbacks.premieres_callbacks  # noqa: F401 - импорт для регистрации декораторов
    from moviebot.bot.callbacks.premieres_callbacks import register_premieres_callbacks
    register_premieres_callbacks(bot)  # Пустая функция, но импорт модуля уже зарегистрировал handlers
    logger.info("✅ Callback handlers для премьер зарегистрированы")
    
    # Импортируем модули handlers, которые содержат callback handlers с декораторами
    # Эти handlers регистрируются автоматически при импорте модуля
    import moviebot.bot.handlers.admin  # noqa: F401 - импорт для регистрации callback handlers
    logger.info("✅ Callback handlers для админских команд зарегистрированы")
    
    import moviebot.bot.handlers.promo  # noqa: F401 - импорт для регистрации callback handlers
    logger.info("✅ Callback handlers для промокодов зарегистрированы")
    
    # handlers/series.py, handlers/start.py, handlers/plan.py, handlers/rate.py, handlers/list.py
    # уже импортируются через register_*_handlers, но их callback handlers используют декораторы напрямую
    # поэтому они регистрируются автоматически при импорте модулей выше
    
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

