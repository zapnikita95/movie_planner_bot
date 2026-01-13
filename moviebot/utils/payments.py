"""
Утилиты для работы с платежами
"""
import logging
import telebot

logger = logging.getLogger(__name__)


def create_stars_invoice(bot, chat_id, title, description, payload, stars_amount, provider_token='', subscription_period=None):
    """Создаёт инвойс для оплаты через Telegram Stars
    
    Args:
        bot: экземпляр TeleBot
        chat_id: ID чата для отправки инвойса
        title: название товара/услуги
        description: описание товара/услуги
        payload: уникальный идентификатор платежа
        stars_amount: количество звезд
        provider_token: для Stars должен быть пустой строкой '' (не None!) - игнорируется, всегда ''
        subscription_period: период подписки в секундах (30*24*60*60 для месячной подписки)
                           Если указан, создается подписка (recurring), иначе разовый платеж
    
    Returns:
        True если инвойс успешно отправлен, False в противном случае
    """
    try:
        logger.info(f"[STARS INVOICE] Создание инвойса: chat_id={chat_id}, title={title}, stars_amount={stars_amount}, payload={payload}")
        
        prices = [telebot.types.LabeledPrice(label=description, amount=stars_amount)]
        
        invoice_params = {
            'chat_id': chat_id,
            'title': title,
            'description': description,
            'invoice_payload': payload,
            'provider_token': '',  # ЯВНО пустая строка для Stars
            'currency': 'XTR',
            'prices': prices,
            'start_parameter': payload[:64] if len(payload) > 64 else payload  # start_parameter ограничен 64 символами
        }
        
        # ПРИМЕЧАНИЕ: subscription_period не поддерживается в send_invoice текущей версией pyTelegramBotAPI
        # Для подписок через Stars подписки обрабатываются через scheduler на основе period_type из базы данных
        # Если указан subscription_period, логируем это, но не передаем в send_invoice
        if subscription_period:
            logger.info(f"[STARS INVOICE] Подписка будет обработана через scheduler (period={subscription_period} секунд, не передается в send_invoice)")
        
        logger.info(f"[STARS INVOICE] Параметры инвойса: provider_token='{invoice_params['provider_token']}', currency='{invoice_params['currency']}', stars_amount={stars_amount}, payload='{payload}'")
        
        bot.send_invoice(**invoice_params)
        
        logger.info("[STARS INVOICE] Инвойс отправлен успешно")
        return True
        
    except telebot.apihelper.ApiTelegramException as tele_e:
        logger.error(f"[STARS INVOICE] Telegram API ошибка: {tele_e}", exc_info=True)
        error_code = getattr(tele_e, 'error_code', None)
        result_json = getattr(tele_e, 'result_json', None)
        description = result_json.get('description') if result_json and isinstance(result_json, dict) else 'N/A'
        logger.error(f"[STARS INVOICE] error_code={error_code}, description={description}")
        try:
            bot.send_message(chat_id, "❌ Ошибка создания платежа (Telegram). Попробуйте позже.")
        except:
            pass
        return False
        
    except Exception as e:
        logger.error(f"[STARS INVOICE] Критическая ошибка: {e}", exc_info=True)
        try:
            bot.send_message(chat_id, "❌ Не удалось создать платёж. Попробуйте позже.")
        except:
            pass
        return False

