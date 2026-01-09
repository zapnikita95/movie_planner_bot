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
        provider_token = provider_token or ''  # Гарантируем, что это пустая строка
        
        invoice_params = {
            'chat_id': chat_id,
            'title': title,
            'description': description,
            'invoice_payload': payload,
            'provider_token': provider_token,  # Для Stars должна быть пустая строка ''
            'currency': 'XTR',  # XTR - валюта Telegram Stars
            'prices': [telebot.types.LabeledPrice(label=description, amount=stars_amount)],  # amount в звездах
            'start_parameter': payload[:64] if len(payload) > 64 else payload  # start_parameter ограничен 64 символами
        }
        
        # Если указан subscription_period, добавляем параметр для создания подписки
        # Согласно документации: https://core.telegram.org/api/subscriptions#bot-subscriptions
        # subscription_period должен быть 30*24*60*60 (месяц) для подписок
        if subscription_period:
            invoice_params['subscription_period'] = subscription_period
            logger.info(f"[STARS] Создается подписка с периодом {subscription_period} секунд")
        
        bot.send_invoice(**invoice_params)
        return True
    except Exception as e:
        logger.error(f"[STARS] Ошибка создания инвойса через Stars: {e}", exc_info=True)
        return False

