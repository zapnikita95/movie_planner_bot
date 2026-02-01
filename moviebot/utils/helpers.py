"""
Вспомогательные функции для проверки доступа к функциям
"""
import logging
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)

FREE_SERIES_LIMIT = 3  # первые N сериалов — полный функционал бесплатно
FREE_TICKET_PLANS_LIMIT = 3  # первые N планов с билетами — полный функционал бесплатно


def _get_first_series_film_ids(chat_id):
    """ID первых FREE_SERIES_LIMIT сериалов в чате (по порядку добавления)"""
    from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
    conn = get_db_connection()
    cur = None
    try:
        with db_lock:
            cur = conn.cursor()
            cur.execute(
                "SELECT id FROM movies WHERE chat_id = %s AND is_series = 1 ORDER BY id ASC LIMIT %s",
                (chat_id, FREE_SERIES_LIMIT)
            )
            rows = cur.fetchall()
        return {r.get('id') if isinstance(r, dict) else r[0] for r in rows} if rows else set()
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass


def has_series_features_access(chat_id, user_id, film_id=None):
    """
    Доступ к уведомлениям и отметке серий.
    True если: есть платная подписка ИЛИ сериал в числе первых FREE_SERIES_LIMIT.
    film_id — id фильма в movies (для проверки «в первых трёх»). Если None — проверяем «следующий будет бесплатным» (count < limit).
    """
    if has_notifications_access(chat_id, user_id):
        return True
    if film_id is not None:
        free_ids = _get_first_series_film_ids(chat_id)
        return film_id in free_ids
    # Сериал ещё не в базе — доступ, если добавление не упрётся в лимит (count < limit)
    from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
    conn = get_db_connection()
    cur = None
    try:
        with db_lock:
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) as cnt FROM movies WHERE chat_id = %s AND is_series = 1",
                (chat_id,)
            )
            row = cur.fetchone()
        cnt = (row.get('cnt') if isinstance(row, dict) else row[0]) or 0
        return cnt < FREE_SERIES_LIMIT
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass


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


def maybe_send_series_limit_message(bot, chat_id, user_id, message_thread_id=None):
    """
    Если в чате ровно 4 сериала — отправить сообщение о лимите с кнопками подписок.
    Вызывать после добавления сериала (ensure_movie_in_database вернул was_inserted=True).
    """
    from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    conn = get_db_connection()
    cur = None
    try:
        with db_lock:
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) as cnt FROM movies WHERE chat_id = %s AND is_series = 1",
                (chat_id,)
            )
            row = cur.fetchone()
        cnt = (row.get('cnt') if isinstance(row, dict) else row[0]) or 0
        if cnt != 4:
            return
        text = (
            "Вы добавили 4-й сериал 😎\n\n"
            "Уведомления о новых сериях и отметка эпизодов работают только для первых 3 сериалов.\n\n"
            "Выберите вариант подписки:"
        )
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("🔔 Уведомления о сериалах", callback_data="payment:subscribe:personal:notifications:month"))
        markup.add(InlineKeyboardButton("💎 Movie Planner PRO", callback_data="payment:tariffs:personal"))
        markup.add(InlineKeyboardButton("💰 Все тарифы", callback_data="payment:tariffs"))
        kw = {'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML', 'reply_markup': markup}
        if message_thread_id is not None:
            kw['message_thread_id'] = message_thread_id
        bot.send_message(**kw)
        logger.info(f"[SERIES LIMIT] Отправлено сообщение о лимите для chat_id={chat_id}, user_id={user_id}")
    except Exception as e:
        logger.error(f"[SERIES LIMIT] Ошибка отправки: {e}", exc_info=True)
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass


def _has_ticket_subscription(chat_id, user_id):
    """Есть подписка 'tickets' или 'all' (для полного доступа к билетам)"""
    from moviebot.database.db_operations import get_user_personal_subscriptions
    personal_subs = get_user_personal_subscriptions(user_id)
    if personal_subs:
        for sub in personal_subs:
            plan_type = sub.get('plan_type')
            expires_at = sub.get('expires_at')
            if plan_type in ['tickets', 'all']:
                if expires_at is None:
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
                except Exception:
                    pass
    if chat_id < 0:
        from moviebot.database.db_operations import get_active_group_subscription_by_chat_id, get_subscription_members
        group_sub = get_active_group_subscription_by_chat_id(chat_id)
        if group_sub and group_sub.get('plan_type') in ['tickets', 'all']:
            exp = group_sub.get('expires_at')
            if exp is not None:
                try:
                    now = datetime.now(pytz.UTC)
                    if isinstance(exp, str):
                        exp = datetime.fromisoformat(exp.replace('Z', '+00:00'))
                    if exp.tzinfo is None:
                        exp = pytz.UTC.localize(exp)
                    if exp <= now:
                        return False
                except Exception:
                    return False
            group_size = group_sub.get('group_size')
            sub_id = group_sub.get('id')
            if group_size and sub_id:
                try:
                    members = get_subscription_members(sub_id)
                    return bool(members and user_id in members)
                except Exception:
                    return False
            return True
    return False


def has_ticket_features_access(chat_id, user_id):
    """
    Доступ к добавлению билетов и напоминаниям.
    True если: подписка tickets/all ИЛИ планов с билетами < FREE_TICKET_PLANS_LIMIT.
    """
    if _has_ticket_subscription(chat_id, user_id):
        return True
    from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
    conn = get_db_connection()
    cur = None
    try:
        with db_lock:
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) as cnt FROM plans WHERE chat_id = %s AND ticket_file_id IS NOT NULL",
                (chat_id,)
            )
            row = cur.fetchone()
        cnt = (row.get('cnt') if isinstance(row, dict) else row[0]) or 0
        return cnt < FREE_TICKET_PLANS_LIMIT
    finally:
        if cur:
            try:
                cur.close()
            except Exception:
                pass


def maybe_send_ticket_limit_message(bot, chat_id, user_id, message_thread_id=None):
    """Сообщение о лимите билетов с кнопками тарифов."""
    text = (
        "Вы уже запланировали 3 похода в кино с билетами — круто! 🎟️\n\n"
        "Полный доступ к напоминаниям и билетам для всех планов — по подписке. Продолжить?"
    )
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("🔔 Уведомления о сериалах", callback_data="payment:subscribe:personal:notifications:month"))
    markup.add(InlineKeyboardButton("🎟 Билеты в кино", callback_data="payment:subscribe:personal:tickets:month"))
    markup.add(InlineKeyboardButton("💎 Movie Planner PRO", callback_data="payment:tariffs:personal"))
    markup.add(InlineKeyboardButton("💰 Все тарифы", callback_data="payment:tariffs"))
    kw = {'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML', 'reply_markup': markup}
    if message_thread_id is not None:
        kw['message_thread_id'] = message_thread_id
    try:
        bot.send_message(**kw)
        logger.info(f"[TICKET LIMIT] Отправлено сообщение о лимите для chat_id={chat_id}")
    except Exception as e:
        logger.error(f"[TICKET LIMIT] Ошибка отправки: {e}", exc_info=True)


def has_tickets_access(chat_id, user_id):
    """Проверяет доступ к билетам: в личных чатах — для всех; в группах — только с подпиской 💎 Movie Planner PRO (plan_type 'all')."""
    from moviebot.database.db_operations import get_user_personal_subscriptions

    # В личных чатах билеты доступны всем
    if chat_id > 0:
        return True

    # В групповых чатах требуется подписка Movie Planner PRO (all)
    personal_subs = get_user_personal_subscriptions(user_id)
    logger.info(f"[HELPERS] has_tickets_access: проверка для user_id={user_id}, chat_id={chat_id}, personal_subs={len(personal_subs) if personal_subs else 0}")
    if personal_subs:
        for sub in personal_subs:
            plan_type = sub.get('plan_type')
            expires_at = sub.get('expires_at')
            logger.info(f"[HELPERS] has_tickets_access: проверка подписки plan_type={plan_type}, expires_at={expires_at}")
            if plan_type == 'all':
                if expires_at is None:  # lifetime
                    logger.info(f"[HELPERS] has_tickets_access: ✅ найдена lifetime подписка {plan_type} для user_id={user_id}, chat_id={chat_id}")
                    return True
                try:
                    now = datetime.now(pytz.UTC)
                    if isinstance(expires_at, datetime):
                        if expires_at.tzinfo is None:
                            expires_at = pytz.UTC.localize(expires_at)
                        if expires_at > now:
                            logger.info(f"[HELPERS] has_tickets_access: ✅ найдена активная подписка {plan_type} для user_id={user_id}, chat_id={chat_id}, expires_at={expires_at}")
                            return True
                        else:
                            logger.warning(f"[HELPERS] has_tickets_access: ❌ подписка {plan_type} истекла для user_id={user_id}, chat_id={chat_id}, expires_at={expires_at}, now={now}")
                    elif isinstance(expires_at, str):
                        expires_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                        if expires_dt.tzinfo is None:
                            expires_dt = pytz.UTC.localize(expires_dt)
                        if expires_dt > now:
                            logger.info(f"[HELPERS] has_tickets_access: ✅ найдена активная подписка {plan_type} для user_id={user_id}, chat_id={chat_id}, expires_at={expires_dt}")
                            return True
                        else:
                            logger.warning(f"[HELPERS] has_tickets_access: ❌ подписка {plan_type} истекла для user_id={user_id}, chat_id={chat_id}, expires_at={expires_dt}, now={now}")
                except Exception as e:
                    logger.warning(f"[HELPERS] has_tickets_access: ошибка проверки expires_at для user_id={user_id}, chat_id={chat_id}, plan_type={plan_type}: {e}", exc_info=True)
                    pass
    
    # Проверяем групповую подписку (для групповых чатов)
    if chat_id < 0:  # группа
        from moviebot.database.db_operations import get_active_group_subscription_by_chat_id, get_subscription_members
        group_sub = get_active_group_subscription_by_chat_id(chat_id)
        logger.info(f"[HELPERS] has_tickets_access: проверка групповой подписки для chat_id={chat_id}, group_sub={group_sub is not None}")
        if group_sub:
            plan_type = group_sub.get('plan_type')
            group_size = group_sub.get('group_size')
            subscription_id = group_sub.get('id')
            expires_at = group_sub.get('expires_at')
            logger.info(f"[HELPERS] has_tickets_access: групповая подписка plan_type={plan_type}, group_size={group_size}, subscription_id={subscription_id}, expires_at={expires_at}")
            
            if plan_type == 'all':
                # Проверяем срок действия подписки
                if expires_at is None:  # lifetime
                    logger.info(f"[HELPERS] has_tickets_access: найдена lifetime групповая подписка {plan_type}")
                else:
                    try:
                        now = datetime.now(pytz.UTC)
                        if isinstance(expires_at, datetime):
                            if expires_at.tzinfo is None:
                                expires_at = pytz.UTC.localize(expires_at)
                            if expires_at <= now:
                                logger.warning(f"[HELPERS] has_tickets_access: ❌ групповая подписка {plan_type} истекла, expires_at={expires_at}, now={now}")
                                return False
                        elif isinstance(expires_at, str):
                            expires_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                            if expires_dt.tzinfo is None:
                                expires_dt = pytz.UTC.localize(expires_dt)
                            if expires_dt <= now:
                                logger.warning(f"[HELPERS] has_tickets_access: ❌ групповая подписка {plan_type} истекла, expires_at={expires_dt}, now={now}")
                                return False
                    except Exception as e:
                        logger.warning(f"[HELPERS] has_tickets_access: ошибка проверки expires_at групповой подписки: {e}", exc_info=True)
                        return False
                
                # Если есть ограничение по участникам (group_size), проверяем, является ли пользователь участником
                if group_size is not None and subscription_id:
                    try:
                        members = get_subscription_members(subscription_id)
                        logger.info(f"[HELPERS] has_tickets_access: участники подписки {subscription_id}: {members}, проверяем user_id={user_id} (тип: {type(user_id)})")
                        if members and user_id in members:
                            logger.info(f"[HELPERS] has_tickets_access: ✅ доступ разрешен для user_id={user_id} в группе chat_id={chat_id} (подписка {subscription_id}, plan_type={plan_type})")
                            return True
                        # Если пользователь не в списке участников, нет доступа
                        logger.warning(f"[HELPERS] has_tickets_access: ❌ доступ запрещен для user_id={user_id} в группе chat_id={chat_id} (подписка {subscription_id}, user_id не в списке участников)")
                        return False
                    except Exception as e:
                        logger.error(f"[HELPERS] Ошибка проверки участников подписки: {e}", exc_info=True)
                        return False
                else:
                    # Если нет ограничения по участникам, доступ есть для всех
                    logger.info(f"[HELPERS] has_tickets_access: ✅ доступ разрешен для всех в группе chat_id={chat_id} (нет ограничения по участникам, plan_type={plan_type})")
                    return True
    
    logger.warning(f"[HELPERS] has_tickets_access: ❌ доступ запрещен для user_id={user_id}, chat_id={chat_id}")
    return False


def has_pro_access(chat_id, user_id):
    """Проверяет доступ к функциям 💎 Movie Planner PRO (подписка plan_type 'all': настройки напоминаний, импорт базы и т.д.)."""
    from moviebot.database.db_operations import get_user_personal_subscriptions

    personal_subs = get_user_personal_subscriptions(user_id)
    if personal_subs:
        for sub in personal_subs:
            plan_type = sub.get('plan_type')
            expires_at = sub.get('expires_at')
            if plan_type == 'all':
                if expires_at is None:
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
                except Exception:
                    pass

    if chat_id < 0:
        from moviebot.database.db_operations import get_active_group_subscription_by_chat_id, get_subscription_members
        group_sub = get_active_group_subscription_by_chat_id(chat_id)
        if group_sub and group_sub.get('plan_type') == 'all':
            expires_at = group_sub.get('expires_at')
            if expires_at is None:
                pass  # check members below
            else:
                try:
                    now = datetime.now(pytz.UTC)
                    if isinstance(expires_at, datetime):
                        if expires_at.tzinfo is None:
                            expires_at = pytz.UTC.localize(expires_at)
                        if expires_at <= now:
                            return False
                    elif isinstance(expires_at, str):
                        expires_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                        if expires_dt.tzinfo is None:
                            expires_dt = pytz.UTC.localize(expires_dt)
                        if expires_dt <= now:
                            return False
                except Exception:
                    return False
            group_size = group_sub.get('group_size')
            subscription_id = group_sub.get('id')
            if group_size is not None and subscription_id:
                try:
                    members = get_subscription_members(subscription_id)
                    if members and user_id in members:
                        return True
                    return False
                except Exception:
                    return False
            return True

    return False


def has_recommendations_access(chat_id, user_id):
    """Проверяет, есть ли у пользователя доступ к функциям рекомендаций
    (требуется подписка 'recommendations' или 'all')
    """
    from moviebot.database.db_operations import get_user_personal_subscriptions

    # Проверяем личную подписку
    personal_subs = get_user_personal_subscriptions(user_id)
    logger.info(f"[HELPERS] has_recommendations_access: проверка для user_id={user_id}, chat_id={chat_id}, personal_subs={len(personal_subs) if personal_subs else 0}")
    if personal_subs:
        for sub in personal_subs:
            plan_type = sub.get('plan_type')
            expires_at = sub.get('expires_at')
            logger.info(f"[HELPERS] has_recommendations_access: проверка подписки plan_type={plan_type}, expires_at={expires_at}")
            if plan_type in ['recommendations', 'all']:
                if expires_at is None:  # lifetime
                    logger.info(f"[HELPERS] has_recommendations_access: ✅ найдена lifetime подписка {plan_type} для user_id={user_id}, chat_id={chat_id}")
                    return True
                try:
                    now = datetime.now(pytz.UTC)
                    if isinstance(expires_at, datetime):
                        if expires_at.tzinfo is None:
                            expires_at = pytz.UTC.localize(expires_at)
                        if expires_at > now:
                            logger.info(f"[HELPERS] has_recommendations_access: ✅ найдена активная подписка {plan_type} для user_id={user_id}, chat_id={chat_id}, expires_at={expires_at}")
                            return True
                        else:
                            logger.warning(f"[HELPERS] has_recommendations_access: ❌ подписка {plan_type} истекла для user_id={user_id}, chat_id={chat_id}, expires_at={expires_at}, now={now}")
                    elif isinstance(expires_at, str):
                        expires_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                        if expires_dt.tzinfo is None:
                            expires_dt = pytz.UTC.localize(expires_dt)
                        if expires_dt > now:
                            logger.info(f"[HELPERS] has_recommendations_access: ✅ найдена активная подписка {plan_type} для user_id={user_id}, chat_id={chat_id}, expires_at={expires_dt}")
                            return True
                        else:
                            logger.warning(f"[HELPERS] has_recommendations_access: ❌ подписка {plan_type} истекла для user_id={user_id}, chat_id={chat_id}, expires_at={expires_dt}, now={now}")
                except Exception as e:
                    logger.warning(f"[HELPERS] has_recommendations_access: ошибка проверки expires_at для user_id={user_id}, chat_id={chat_id}, plan_type={plan_type}: {e}", exc_info=True)
                    pass
    
    # Проверяем групповую подписку
    if chat_id < 0:
        from moviebot.database.db_operations import get_active_group_subscription_by_chat_id, get_subscription_members
        group_sub = get_active_group_subscription_by_chat_id(chat_id)
        logger.info(f"[HELPERS] has_recommendations_access: проверка групповой подписки для chat_id={chat_id}, group_sub={group_sub is not None}")
        if group_sub:
            plan_type = group_sub.get('plan_type')
            group_size = group_sub.get('group_size')
            subscription_id = group_sub.get('id')
            expires_at = group_sub.get('expires_at')
            logger.info(f"[HELPERS] has_recommendations_access: групповая подписка plan_type={plan_type}, group_size={group_size}, subscription_id={subscription_id}, expires_at={expires_at}")
            
            if plan_type in ['recommendations', 'all']:
                # Проверяем срок действия подписки
                if expires_at is None:  # lifetime
                    logger.info(f"[HELPERS] has_recommendations_access: найдена lifetime групповая подписка {plan_type}")
                else:
                    try:
                        now = datetime.now(pytz.UTC)
                        if isinstance(expires_at, datetime):
                            if expires_at.tzinfo is None:
                                expires_at = pytz.UTC.localize(expires_at)
                            if expires_at <= now:
                                logger.warning(f"[HELPERS] has_recommendations_access: ❌ групповая подписка {plan_type} истекла, expires_at={expires_at}, now={now}")
                                return False
                        elif isinstance(expires_at, str):
                            expires_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                            if expires_dt.tzinfo is None:
                                expires_dt = pytz.UTC.localize(expires_dt)
                            if expires_dt <= now:
                                logger.warning(f"[HELPERS] has_recommendations_access: ❌ групповая подписка {plan_type} истекла, expires_at={expires_dt}, now={now}")
                                return False
                    except Exception as e:
                        logger.warning(f"[HELPERS] has_recommendations_access: ошибка проверки expires_at групповой подписки: {e}", exc_info=True)
                        return False
                
                # Если есть ограничение по участникам (group_size), проверяем, является ли пользователь участником
                if group_size is not None and subscription_id:
                    try:
                        members = get_subscription_members(subscription_id)
                        logger.info(f"[HELPERS] has_recommendations_access: участники подписки {subscription_id}: {members}, проверяем user_id={user_id} (тип: {type(user_id)})")
                        if members and user_id in members:
                            logger.info(f"[HELPERS] has_recommendations_access: ✅ доступ разрешен для user_id={user_id} в группе chat_id={chat_id} (подписка {subscription_id}, plan_type={plan_type})")
                            return True
                        # Если пользователь не в списке участников, нет доступа
                        logger.warning(f"[HELPERS] has_recommendations_access: ❌ доступ запрещен для user_id={user_id} в группе chat_id={chat_id} (подписка {subscription_id}, user_id не в списке участников)")
                        return False
                    except Exception as e:
                        logger.error(f"[HELPERS] Ошибка проверки участников подписки: {e}", exc_info=True)
                        return False
                else:
                    # Если нет ограничения по участникам, доступ есть для всех
                    logger.info(f"[HELPERS] has_recommendations_access: ✅ доступ разрешен для всех в группе chat_id={chat_id} (нет ограничения по участникам, plan_type={plan_type})")
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