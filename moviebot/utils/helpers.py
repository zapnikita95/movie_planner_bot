"""
Вспомогательные функции для проверки доступа к функциям
"""
import logging
from moviebot.database.db_operations import (
    get_user_personal_subscriptions, 
    get_active_group_subscription_by_chat_id,
    get_active_subscription
)


def has_notifications_access(chat_id, user_id=None):
    """
    Проверяет доступ к уведомлениям о сериалах.
    - В личке (user_id есть) — проверяем подписку пользователя
    - В группе (user_id=None) — проверяем подписку группы
    """
    from moviebot.database.db_connection import db_lock, get_db_cursor
    
    try:
        with db_lock:
            cursor = get_db_cursor() 
            if user_id is not None:
                # Личка — проверяем подписку пользователя
                cursor.execute(
                    """
                    SELECT 1 FROM subscriptions 
                    WHERE chat_id = %s AND user_id = %s AND active = TRUE AND expires_at > NOW()
                    LIMIT 1
                    """,
                    (chat_id, user_id)
                )
            else:
                # Группа — проверяем подписку чата
                cursor.execute(
                    """
                    SELECT 1 FROM subscriptions 
                    WHERE chat_id = %s AND user_id IS NULL AND is_active = TRUE AND expires_at > NOW()
                    LIMIT 1
                    """,
                    (chat_id,)
                )
            return cursor.fetchone() is not None
    except Exception as e:
        logger.error(f"[NOTIFICATIONS ACCESS] Ошибка проверки: {e}", exc_info=True)
        return False


def has_tickets_access(chat_id, user_id):
    """Проверяет, есть ли у пользователя доступ к функциям билетов в кино
    (требуется подписка 'tickets' или 'all')
    """
    # Убрана проверка для создателя - теперь даже создатель должен иметь подписку
    
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
    # Убрана проверка для создателя - теперь даже создатель должен иметь подписку
    
    # Проверяем личную подписку - используем get_user_personal_subscriptions для проверки всех личных подписок
    # Это важно, так как личная подписка должна работать независимо от того, в каком чате пользователь
    # get_user_personal_subscriptions уже возвращает только активные подписки (is_active = TRUE AND expires_at > NOW())
    personal_subs = get_user_personal_subscriptions(user_id)
    if personal_subs:
        for sub in personal_subs:
            plan_type = sub.get('plan_type')
            if plan_type in ['recommendations', 'all']:
                # Дополнительно проверяем, что подписка не истекла
                expires_at = sub.get('expires_at')
                if expires_at is None:
                    # Подписка без срока действия (lifetime)
                    return True
                else:
                    # Проверяем, что подписка не истекла
                    from datetime import datetime
                    import pytz
                    now = datetime.now(pytz.UTC)
                    if isinstance(expires_at, datetime):
                        if expires_at.tzinfo is None:
                            expires_at = pytz.UTC.localize(expires_at)
                        if expires_at > now:
                            return True
                    elif isinstance(expires_at, str):
                        try:
                            expires_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                            if expires_dt.tzinfo is None:
                                expires_dt = pytz.UTC.localize(expires_dt)
                            if expires_dt > now:
                                return True
                        except:
                            pass
    
    # Также проверяем через get_active_subscription для обратной совместимости
    # (на случай, если chat_id == user_id в личном чате)
    personal_sub = get_active_subscription(chat_id, user_id, 'personal')
    if personal_sub:
        plan_type = personal_sub.get('plan_type')
        if plan_type in ['recommendations', 'all']:
            # get_active_subscription уже проверяет активность подписки
            return True
    
    # Проверяем групповую подписку (для групповых чатов)
    if chat_id < 0:  # Групповой чат
        # Для групповых подписок нужно искать подписку по chat_id группы
        group_sub = get_active_group_subscription_by_chat_id(chat_id)
        if group_sub:
            plan_type = group_sub.get('plan_type')
            if plan_type in ['recommendations', 'all']:
                # get_active_group_subscription_by_chat_id уже проверяет активность подписки
                return True
    
    return False

