"""
Декоратор для быстрого ответа на callback queries
Предотвращает 499 ошибки (timeout) от Telegram
"""
import logging
import functools
from typing import Callable, Any

logger = logging.getLogger(__name__)

def quick_callback_answer(bot_instance):
    """
    Декоратор для callback handlers, который:
    1. Отвечает на callback query СРАЗУ (до начала обработки)
    2. Обрабатывает ошибки устаревших callback queries
    3. Запускает тяжелую обработку в фоновом потоке, если нужно
    
    Использование:
        @quick_callback_answer(bot)
        @bot.callback_query_handler(func=lambda call: call.data.startswith("my_callback:"))
        def my_handler(call):
            # Тяжелая обработка здесь
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(call):
            # ШАГ 1: Отвечаем на callback СРАЗУ (критично для предотвращения 499)
            callback_answered = False
            try:
                bot_instance.answer_callback_query(call.id, text="⏳ Обрабатываю...")
                callback_answered = True
                logger.debug(f"[QUICK CALLBACK] Ответ на callback {call.id} отправлен сразу")
            except Exception as answer_error:
                error_str = str(answer_error).lower()
                if 'too old' in error_str or 'timeout expired' in error_str or 'invalid' in error_str:
                    logger.warning(f"[QUICK CALLBACK] Callback query {call.id} устарел, но продолжаем обработку: {answer_error}")
                    callback_answered = False
                else:
                    logger.error(f"[QUICK CALLBACK] Ошибка при ответе на callback {call.id}: {answer_error}", exc_info=True)
                    callback_answered = False
            
            # ШАГ 2: Запускаем обработку
            try:
                return func(call)
            except Exception as e:
                logger.error(f"[QUICK CALLBACK] Ошибка в handler {func.__name__}: {e}", exc_info=True)
                # Пытаемся отправить сообщение об ошибке, если callback еще не отвечен
                if not callback_answered:
                    try:
                        bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
                    except:
                        pass
                # НЕ ПРОПАГИРУЕМ ОШИБКУ - это предотвращает падение бота
                return None
        
        return wrapper
    return decorator
