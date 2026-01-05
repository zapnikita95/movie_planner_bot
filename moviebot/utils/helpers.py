"""
Вспомогательные функции для проверки доступа к функциям
"""
from moviebot.database.db_operations import (
    get_user_personal_subscriptions, 
    get_active_group_subscription_by_chat_id,
    get_active_subscription
)


def has_notifications_access(chat_id, user_id):
    """Проверяет, есть ли у пользователя доступ к функциям уведомлений о сериалах
    (требуется подписка 'notifications' или 'all')
    """
    # Для создателя (user_id 301810276) всегда разрешаем доступ
    if user_id == 301810276:
        return True
    
    # Проверяем личную подписку - используем get_user_personal_subscriptions для проверки всех личных подписок
    # Это важно, так как личная подписка должна работать независимо от того, в каком чате пользователь
    personal_subs = get_user_personal_subscriptions(user_id)
    if personal_subs:
        for sub in personal_subs:
            plan_type = sub.get('plan_type')
            if plan_type in ['notifications', 'all']:
                return True
    
    # Также проверяем через get_active_subscription для обратной совместимости
    # (на случай, если chat_id == user_id в личном чате)
    personal_sub = get_active_subscription(chat_id, user_id, 'personal')
    if personal_sub:
        plan_type = personal_sub.get('plan_type')
        if plan_type in ['notifications', 'all']:
            return True
    
    # Проверяем групповую подписку (для групповых чатов)
    if chat_id < 0:  # Групповой чат
        # Для групповых подписок нужно искать подписку по chat_id группы
        group_sub = get_active_group_subscription_by_chat_id(chat_id)
        if group_sub:
            plan_type = group_sub.get('plan_type')
            if plan_type in ['notifications', 'all']:
                return True
    
    return False


def has_tickets_access(chat_id, user_id):
    """Проверяет, есть ли у пользователя доступ к функциям билетов в кино
    (требуется подписка 'tickets' или 'all')
    """
    # Для создателя (user_id 301810276) всегда разрешаем доступ
    if user_id == 301810276:
        return True
    
    # Проверяем личную подписку - используем get_user_personal_subscriptions для проверки всех личных подписок
    # Это важно, так как личная подписка должна работать независимо от того, в каком чате пользователь
    personal_subs = get_user_personal_subscriptions(user_id)
    if personal_subs:
        for sub in personal_subs:
            plan_type = sub.get('plan_type')
            if plan_type in ['tickets', 'all']:
                return True
    
    # Также проверяем через get_active_subscription для обратной совместимости
    # (на случай, если chat_id == user_id в личном чате)
    personal_sub = get_active_subscription(chat_id, user_id, 'personal')
    if personal_sub:
        plan_type = personal_sub.get('plan_type')
        if plan_type in ['tickets', 'all']:
            return True
    
    # Проверяем групповую подписку (для групповых чатов)
    if chat_id < 0:  # Групповой чат
        # Для групповых подписок нужно искать подписку по chat_id группы
        group_sub = get_active_group_subscription_by_chat_id(chat_id)
        if group_sub:
            plan_type = group_sub.get('plan_type')
            if plan_type in ['tickets', 'all']:
                return True
    
    return False


def has_recommendations_access(chat_id, user_id):
    """Проверяет, есть ли у пользователя доступ к функциям рекомендаций
    (требуется подписка 'recommendations' или 'all')
    """
    # Для создателя (user_id 301810276) всегда разрешаем доступ
    if user_id == 301810276:
        return True
    
    # Проверяем личную подписку - используем get_user_personal_subscriptions для проверки всех личных подписок
    # Это важно, так как личная подписка должна работать независимо от того, в каком чате пользователь
    personal_subs = get_user_personal_subscriptions(user_id)
    if personal_subs:
        for sub in personal_subs:
            plan_type = sub.get('plan_type')
            if plan_type in ['recommendations', 'all']:
                return True
    
    # Также проверяем через get_active_subscription для обратной совместимости
    # (на случай, если chat_id == user_id в личном чате)
    personal_sub = get_active_subscription(chat_id, user_id, 'personal')
    if personal_sub:
        plan_type = personal_sub.get('plan_type')
        if plan_type in ['recommendations', 'all']:
            return True
    
    # Проверяем групповую подписку (для групповых чатов)
    if chat_id < 0:  # Групповой чат
        # Для групповых подписок нужно искать подписку по chat_id группы
        group_sub = get_active_group_subscription_by_chat_id(chat_id)
        if group_sub:
            plan_type = group_sub.get('plan_type')
            if plan_type in ['recommendations', 'all']:
                return True
    
    return False

