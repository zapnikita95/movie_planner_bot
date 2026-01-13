"""
Утилиты для работы с платежами
"""
import logging
import telebot

logger = logging.getLogger(__name__)


def create_stars_invoice(bot, chat_id, title, description, payload, stars_amount, provider_token='', subscription_period=None):
    """Создает инвойс для оплаты через Telegram Stars
    
    Args:
        bot: экземпляр TeleBot
        chat_id: ID чата для отправки инвойса
        title: название товара/услуги
        description: описание товара/услуги
        payload: уникальный идентификатор платежа
        stars_amount: количество звезд
        provider_token: для Stars должен быть пустой строкой '' (не None!)
        subscription_period: период подписки в секундах (30*24*60*60 для месячной подписки)
                           Если указан, создается подписка (recurring), иначе разовый платеж
    
    Returns:
        True если инвойс успешно отправлен, False в противном случае
    """
    try:
        # Для Telegram Stars используем валюту XTR
        # Согласно документации: https://core.telegram.org/bots/api#sendinvoice
        # КРИТИЧНО: provider_token должен быть пустой строкой '', а не None
        # Если None - Telegram может интерпретировать как обычный платеж и ждать pre_checkout
        # Явно устанавливаем пустую строку для Stars
        provider_token = ''  # КРИТИЧНО: для Stars всегда пустая строка
        logger.info(f"[STARS] Создание инвойса: provider_token='{provider_token}' (пустая строка для Stars), currency=XTR, stars_amount={stars_amount}")
        
        invoice_params = {
            'chat_id': chat_id,
            'title': title,
            'description': description,
            'invoice_payload': payload,
            'provider_token': provider_token,  # Для Stars всегда пустая строка ''
            'currency': 'XTR',  # XTR - валюта Telegram Stars
            'prices': [telebot.types.LabeledPrice(label=description, amount=stars_amount)],  # amount в звездах
            'start_parameter': payload[:64] if len(payload) > 64 else payload  # start_parameter ограничен 64 символами
        }
        
        # ПРИМЕЧАНИЕ: subscription_period не поддерживается в send_invoice текущей версией pyTelegramBotAPI
        # Для подписок через Stars подписки обрабатываются через scheduler на основе period_type из базы данных
        # Если указан subscription_period, логируем это, но не передаем в send_invoice
        if subscription_period:
            logger.info(f"[STARS] Подписка будет обработана через scheduler (period={subscription_period} секунд, не передается в send_invoice)")
        
        logger.info(f"[STARS] Вызов bot.send_invoice с параметрами: provider_token='{provider_token}', currency='XTR'")
        bot.send_invoice(**invoice_params)
        logger.info(f"[STARS] bot.send_invoice успешно вызван")
        return True
    except Exception as e:
        logger.error(f"[STARS] Ошибка создания инвойса через Stars: {e}", exc_info=True)
        return False

