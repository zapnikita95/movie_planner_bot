"""
Вспомогательные функции для проверки доступа к функциям
"""
import logging
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

def has_notifications_access(chat_id, user_id):
    """Проверяет, есть ли у пользователя доступ к функциям уведомлений
    (требуется подписка 'notifications' или 'all')
    """
    from moviebot.database.db_operations import get_user_personal_subscriptions

    # Проверяем личную подписку
    personal_subs = get_user_personal_subscriptions(user_id)
    if personal_subs:
        for sub in personal_subs:
            plan_type = sub.get('plan_type')
            expires_at = sub.get('expires_at')
            if plan_type in ['notifications', 'all']:
                if expires_at is None:  # lifetime
                    return True
                try:
                    now = datetime.now(pytz.UTC)
                    if isinstance(expires_at, datetime):
                        if expires_at.tzinfo is None:
                            expires_at = pytz.UTC.localize(expires_at)
                        if expires_at > now:
                            return True
                    elif isinstance(expires_at, str):
                        expires_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                        if expires_dt.tzinfo is None:
                            expires_dt = pytz.UTC.localize(expires_dt)
                        if expires_dt > now:
                            return True
                except:
                    pass  # если дата кривая — пропускаем
    
    # Проверяем групповую подписку (для групповых чатов)
    if chat_id < 0:  # группа
        from moviebot.database.db_operations import get_active_group_subscription_by_chat_id, get_subscription_members
        group_sub = get_active_group_subscription_by_chat_id(chat_id)
        if group_sub:
            plan_type = group_sub.get('plan_type')
            group_size = group_sub.get('group_size')
            subscription_id = group_sub.get('id')
            
            if plan_type in ['notifications', 'all']:
                # Если есть ограничение по участникам (group_size), проверяем, является ли пользователь участником
                if group_size is not None and subscription_id:
                    try:
                        members = get_subscription_members(subscription_id)
                        if members and user_id in members:
                            return True
                        # Если пользователь не в списке участников, нет доступа
                        return False
                    except Exception as e:
                        logger.error(f"[HELPERS] Ошибка проверки участников подписки: {e}", exc_info=True)
                        return False
                else:
                    # Если нет ограничения по участникам, доступ есть для всех
                    return True
    
    return False


def has_tickets_access(chat_id, user_id):
    """Проверяет, есть ли у пользователя доступ к функциям билетов в кино
    (требуется подписка 'tickets' или 'all')
    """
    from moviebot.database.db_operations import get_user_personal_subscriptions
    
    # Проверяем личную подписку
    personal_subs = get_user_personal_subscriptions(user_id)
    if personal_subs:
        for sub in personal_subs:
            plan_type = sub.get('plan_type')
            expires_at = sub.get('expires_at')
            if plan_type in ['tickets', 'all']:
                if expires_at is None:  # lifetime
                    return True
                try:
                    now = datetime.now(pytz.UTC)
                    if isinstance(expires_at, datetime):
                        if expires_at.tzinfo is None:
                            expires_at = pytz.UTC.localize(expires_at)
                        if expires_at > now:
                            return True
                    elif isinstance(expires_at, str):
                        expires_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                        if expires_dt.tzinfo is None:
                            expires_dt = pytz.UTC.localize(expires_dt)
                        if expires_dt > now:
                            return True
                except:
                    pass  # если дата кривая — пропускаем
    
    # Проверяем групповую подписку (для групповых чатов)
    if chat_id < 0:  # группа
        from moviebot.database.db_operations import get_active_group_subscription_by_chat_id, get_subscription_members
        group_sub = get_active_group_subscription_by_chat_id(chat_id)
        if group_sub:
            plan_type = group_sub.get('plan_type')
            group_size = group_sub.get('group_size')
            subscription_id = group_sub.get('id')
            
            if plan_type in ['tickets', 'all']:
                # Если есть ограничение по участникам (group_size), проверяем, является ли пользователь участником
                if group_size is not None and subscription_id:
                    try:
                        members = get_subscription_members(subscription_id)
                        if members and user_id in members:
                            return True
                        # Если пользователь не в списке участников, нет доступа
                        return False
                    except Exception as e:
                        logger.error(f"[HELPERS] Ошибка проверки участников подписки: {e}", exc_info=True)
                        return False
                else:
                    # Если нет ограничения по участникам, доступ есть для всех
                    return True
    
    return False


def has_recommendations_access(chat_id, user_id):
    """Проверяет, есть ли у пользователя доступ к функциям рекомендаций
    (требуется подписка 'recommendations' или 'all')
    """
    from moviebot.database.db_operations import get_user_personal_subscriptions

    # Проверяем личную подписку
    personal_subs = get_user_personal_subscriptions(user_id)
    if personal_subs:
        for sub in personal_subs:
            plan_type = sub.get('plan_type')
            expires_at = sub.get('expires_at')
            if plan_type in ['recommendations', 'all']:
                if expires_at is None:  # lifetime
                    return True
                try:
                    now = datetime.now(pytz.UTC)
                    if isinstance(expires_at, datetime):
                        if expires_at.tzinfo is None:
                            expires_at = pytz.UTC.localize(expires_at)
                        if expires_at > now:
                            return True
                    elif isinstance(expires_at, str):
                        expires_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                        if expires_dt.tzinfo is None:
                            expires_dt = pytz.UTC.localize(expires_dt)
                        if expires_dt > now:
                            return True
                except:
                    pass
    
    # Проверяем групповую подписку
    if chat_id < 0:
        from moviebot.database.db_operations import get_active_group_subscription_by_chat_id, get_subscription_members
        group_sub = get_active_group_subscription_by_chat_id(chat_id)
        if group_sub:
            plan_type = group_sub.get('plan_type')
            group_size = group_sub.get('group_size')
            subscription_id = group_sub.get('id')
            
            if plan_type in ['recommendations', 'all']:
                # Если есть ограничение по участникам (group_size), проверяем, является ли пользователь участником
                if group_size is not None and subscription_id:
                    try:
                        members = get_subscription_members(subscription_id)
                        if members and user_id in members:
                            return True
                        # Если пользователь не в списке участников, нет доступа
                        return False
                    except Exception as e:
                        logger.error(f"[HELPERS] Ошибка проверки участников подписки: {e}", exc_info=True)
                        return False
                else:
                    # Если нет ограничения по участникам, доступ есть для всех
                    return True
    
    return False


def extract_film_info_from_existing(existing):
    """
    Безопасно извлекает film_id и watched из existing (tuple, dict или None)
    Возвращает: (film_id: int|None, watched: bool)
    """
    if not existing:
        return None, False

    logger.debug(f"[EXTRACT EXISTING] Тип: {type(existing)}, значение: {existing}")

    if isinstance(existing, dict):
        return existing.get('id'), existing.get('watched', False)

    if isinstance(existing, tuple):
        film_id = existing[0] if len(existing) > 0 else None
        watched = existing[2] if len(existing) > 2 else False
        return film_id, watched

    logger.warning(f"[EXTRACT EXISTING] Неизвестный тип existing: {type(existing)}")
    return None, False