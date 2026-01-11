"""
Модуль для работы с YooKassa API
"""
import logging
import os
import uuid
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

# Проверяем доступность модуля yookassa
try:
    from yookassa import Configuration, Payment
    YOOKASSA_AVAILABLE = True
except ImportError:
    YOOKASSA_AVAILABLE = False
    logger.warning("Модуль yookassa не установлен. Функции оплаты будут недоступны.")
    # Создаем заглушки для типов
    class Configuration:
        account_id = None
        secret_key = None
    class Payment:
        @staticmethod
        def create(*args, **kwargs):
            raise ImportError("yookassa не установлен")
        @staticmethod
        def find_one(*args, **kwargs):
            raise ImportError("yookassa не установлен")


def init_yookassa(shop_id: Optional[str] = None, secret_key: Optional[str] = None) -> bool:
    """
    Инициализирует YooKassa с учетными данными
    
    Args:
        shop_id: Shop ID YooKassa (если None, берется из переменных окружения)
        secret_key: Secret Key YooKassa (если None, берется из переменных окружения)
    
    Returns:
        True если инициализация успешна, False в противном случае
    """
    if not YOOKASSA_AVAILABLE:
        logger.error("[YOOKASSA] Модуль yookassa не установлен")
        return False
    
    if not shop_id:
        shop_id = os.getenv('YOOKASSA_SHOP_ID', '').strip()
    if not secret_key:
        secret_key = os.getenv('YOOKASSA_SECRET_KEY', '').strip()
    
    if not shop_id or not secret_key:
        logger.error("[YOOKASSA] YOOKASSA_SHOP_ID или YOOKASSA_SECRET_KEY не заданы")
        return False
    
    try:
        Configuration.account_id = shop_id
        Configuration.secret_key = secret_key
        logger.info(f"[YOOKASSA] Инициализация успешна: shop_id={shop_id[:4]}...{shop_id[-4:] if len(shop_id) > 8 else ''}")
        return True
    except Exception as e:
        logger.error(f"[YOOKASSA] Ошибка инициализации: {e}", exc_info=True)
        return False


def create_payment(
    amount: float,
    description: str,
    return_url: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    payment_method_id: Optional[str] = None,
    capture: bool = True,
    save_payment_method: bool = False,  # по умолчанию False, но мы будем передавать True для рекуррентных
    is_recurring: bool = False  # новый параметр для ясности
) -> Optional[Any]:
    """
    Создает платеж в YooKassa
    """
    if not YOOKASSA_AVAILABLE:
        logger.error("[YOOKASSA] Модуль yookassa не установлен")
        return None
    
    if not Configuration.account_id or not Configuration.secret_key:
        if not init_yookassa():
            return None
    
    if not return_url:
        return_url = os.getenv('YOOKASSA_RETURN_URL', 'tg://resolve?domain=movie_planner_bot')
    
    if metadata is None:
        metadata = {}
    
    try:
        payment_data = {
            "amount": {
                "value": f"{amount:.2f}",
                "currency": "RUB"
            },
            "capture": capture,
            "description": description,
            "metadata": metadata
        }
        
        if payment_method_id:
            payment_data["payment_method_id"] = payment_method_id
            logger.info(f"[YOOKASSA] Рекуррентный платёж с payment_method_id={payment_method_id}")
        else:
            payment_data["confirmation"] = {
                "type": "redirect",
                "return_url": return_url
            }
            
            # Для всех рекуррентных подписок (кроме lifetime) — просим сохранить карту
            if save_payment_method or is_recurring:
                payment_data["save_payment_method"] = True
                logger.info(f"[YOOKASSA] save_payment_method=True — просим сохранить карту для автоплатежей (save_payment_method={save_payment_method}, is_recurring={is_recurring})")
        
        payment = Payment.create(payment_data)
        logger.info(f"[YOOKASSA] Платеж создан: id={payment.id}, status={payment.status}")
        return payment
    except Exception as e:
        logger.error(f"[YOOKASSA] Ошибка создания платежа: {e}", exc_info=True)
        return None

def get_payment_info(payment_id: str) -> Optional[Any]:
    """
    Получает информацию о платеже из YooKassa
    
    Args:
        payment_id: ID платежа в YooKassa
    
    Returns:
        Объект Payment или None в случае ошибки
    """
    if not YOOKASSA_AVAILABLE:
        logger.error("[YOOKASSA] Модуль yookassa не установлен")
        return None
    
    # Инициализируем YooKassa, если еще не инициализирован
    if not Configuration.account_id or not Configuration.secret_key:
        if not init_yookassa():
            return None
    
    try:
        payment = Payment.find_one(payment_id)
        logger.info(f"[YOOKASSA] Получена информация о платеже: id={payment.id}, status={payment.status}")
        return payment
    except Exception as e:
        logger.error(f"[YOOKASSA] Ошибка получения информации о платеже: {e}", exc_info=True)
        return None


def create_subscription_payment(
    user_id: int,
    chat_id: int,
    subscription_type: str,
    plan_type: str,
    period_type: str,
    amount: float,
    group_size: Optional[int] = None,
    telegram_username: Optional[str] = None,
    group_username: Optional[str] = None,
    payment_id: Optional[str] = None,
    is_combined: bool = False,
    combine_type: Optional[str] = None,
    existing_subs_ids: Optional[list] = None,
    upgrade_subscription_id: Optional[int] = None,
    upgrade_from_plan: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Создает платеж для подписки
    
    Args:
        user_id: ID пользователя Telegram
        chat_id: ID чата
        subscription_type: Тип подписки ('personal' или 'group')
        plan_type: Тип плана ('notifications', 'recommendations', 'tickets', 'all')
        period_type: Тип периода ('month', '3months', 'year', 'lifetime')
        amount: Сумма платежа
        group_size: Размер группы (для групповых подписок)
        telegram_username: Username пользователя Telegram
        group_username: Username группы
        payment_id: Уникальный ID платежа (если None, генерируется автоматически)
        is_combined: Является ли платеж объединенным
        combine_type: Тип объединения
        existing_subs_ids: Список ID существующих подписок
        upgrade_subscription_id: ID подписки для обновления
        upgrade_from_plan: Старый тип плана при обновлении
    
    Returns:
        Словарь с информацией о платеже: {'payment': Payment, 'payment_id': str, 'confirmation_url': str}
        или None в случае ошибки
    """
    if not payment_id:
        payment_id = str(uuid.uuid4())
    
    # Формируем описание платежа
    period_names = {
        'month': 'месяц',
        '3months': '3 месяца',
        'year': 'год',
        'lifetime': 'навсегда',
        'test': 'тестовый (10 минут)'
    }
    period_name = period_names.get(period_type, period_type)
    
    plan_names = {
        'notifications': 'Уведомления о сериалах',
        'recommendations': 'Персональные рекомендации',
        'tickets': 'Билеты в кино',
        'all': 'Все режимы'
    }
    plan_name = plan_names.get(plan_type, plan_type)
    
    subscription_type_name = 'Личная подписка' if subscription_type == 'personal' else f'Групповая подписка (на {group_size} участников)'
    
    if upgrade_subscription_id:
        description = f"Обновление {subscription_type_name}: {plan_name}, период: {period_name}"
    else:
        description = f"{subscription_type_name}: {plan_name}, период: {period_name}"
    
    # Формируем metadata
    metadata = {
        "user_id": str(user_id),
        "chat_id": str(chat_id),
        "subscription_type": subscription_type,
        "plan_type": plan_type,
        "period_type": period_type,
        "payment_id": payment_id
    }
    
    if group_size:
        metadata["group_size"] = str(group_size)
    if telegram_username:
        metadata["telegram_username"] = telegram_username
    if group_username:
        metadata["group_username"] = group_username
    if is_combined:
        metadata["is_combined"] = "true"
        if combine_type:
            metadata["combine_type"] = combine_type
        if existing_subs_ids:
            metadata["existing_subs_ids"] = ','.join([str(sid) for sid in existing_subs_ids if sid])
    if upgrade_subscription_id:
        metadata["upgrade_subscription_id"] = str(upgrade_subscription_id)
    if upgrade_from_plan:
        metadata["upgrade_from_plan"] = upgrade_from_plan
    
    # Определяем, нужно ли сохранять способ оплаты
    # Сохраняем для всех периодических подписок (не lifetime)
    # Для тестового тарифа тоже сохраняем, чтобы можно было проводить автоплатежи
    save_payment_method = period_type != 'lifetime'
    
    # Создаем платеж
    payment = create_payment(
        amount=amount,
        description=description,
        metadata=metadata,
        save_payment_method=save_payment_method
    )
    
    if not payment:
        return None
    
    # Получаем URL для оплаты
    confirmation_url = None
    if hasattr(payment, 'confirmation') and payment.confirmation:
        if hasattr(payment.confirmation, 'confirmation_url'):
            confirmation_url = payment.confirmation.confirmation_url
    
    return {
        'payment': payment,
        'payment_id': payment_id,
        'confirmation_url': confirmation_url
    }


def create_recurring_payment(
    user_id: int,
    chat_id: int,
    subscription_type: str,
    plan_type: str,
    period_type: str,
    amount: float,
    payment_method_id: str,
    group_size: Optional[int] = None,
    telegram_username: Optional[str] = None,
    group_username: Optional[str] = None
) -> Optional[Any]:
    """
    Создает безакцептный рекуррентный платеж
    
    Args:
        user_id: ID пользователя Telegram
        chat_id: ID чата
        subscription_type: Тип подписки ('personal' или 'group')
        plan_type: Тип плана ('notifications', 'recommendations', 'tickets', 'all')
        period_type: Тип периода ('month', '3months', 'year')
        amount: Сумма платежа
        payment_method_id: ID сохраненного способа оплаты
        group_size: Размер группы (для групповых подписок)
        telegram_username: Username пользователя Telegram
        group_username: Username группы
    
    Returns:
        Объект Payment или None в случае ошибки
    """
    payment_id = str(uuid.uuid4())
    
    # Формируем описание платежа
    period_names = {
        'month': 'месяц',
        '3months': '3 месяца',
        'year': 'год'
    }
    period_name = period_names.get(period_type, period_type)
    
    plan_names = {
        'notifications': 'Уведомления о сериалах',
        'recommendations': 'Персональные рекомендации',
        'tickets': 'Билеты в кино',
        'all': 'Все режимы'
    }
    plan_name = plan_names.get(plan_type, plan_type)
    
    subscription_type_name = 'Личная подписка' if subscription_type == 'personal' else f'Групповая подписка (на {group_size} участников)'
    description = f"{subscription_type_name}: {plan_name}, период: {period_name} (User ID: {user_id})"
    
    # Формируем metadata
    metadata = {
        "user_id": str(user_id),
        "chat_id": str(chat_id),
        "subscription_type": subscription_type,
        "plan_type": plan_type,
        "period_type": period_type,
        "payment_id": payment_id,
        "recurring": "true"
    }
    if group_size:
        metadata["group_size"] = str(group_size)
    if telegram_username:
        metadata["telegram_username"] = telegram_username
    if group_username:
        metadata["group_username"] = group_username
    
    # Создаем безакцептный платеж
    payment = create_payment(
        amount=amount,
        description=description,
        metadata=metadata,
        payment_method_id=payment_method_id,
        capture=True
    )
    
    return payment

