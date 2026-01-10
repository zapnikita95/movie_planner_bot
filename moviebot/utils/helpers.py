"""
Вспомогательные функции для проверки доступа к функциям
"""
import logging
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

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
                    WHERE chat_id = %s AND user_id = %s AND is_active = TRUE 
                    AND (expires_at IS NULL OR expires_at > NOW())
                    LIMIT 1
                    """,
                    (chat_id, user_id)
                )
            else:
                # Группа — проверяем подписку чата
                cursor.execute(
                    """
                    SELECT 1 FROM subscriptions 
                    WHERE chat_id = %s AND user_id IS NULL AND is_active = TRUE 
                    AND (expires_at IS NULL OR expires_at > NOW())
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
        from moviebot.database.db_operations import get_active_group_subscription_by_chat_id
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
        from moviebot.database.db_operations import get_active_group_subscription_by_chat_id
        group_sub = get_active_group_subscription_by_chat_id(chat_id)
        if group_sub:
            plan_type = group_sub.get('plan_type')
            if plan_type in ['recommendations', 'all']:
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