"""
Callback handlers для работы с платежами
"""
import logging
import os
import uuid
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.bot.bot_init import bot
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.database.db_operations import (
    get_active_subscription, save_payment, create_subscription,
    get_user_personal_subscriptions, get_user_group_subscriptions,
    cancel_subscription, get_active_group_users, get_subscription_by_id,
    get_user_groups, get_subscription_members, update_subscription_group_size,
    get_active_subscription_by_username, get_active_group_subscription,
    has_subscription_feature
)
from moviebot.bot.bot_init import BOT_ID
from moviebot.api.yookassa_api import create_subscription_payment, YOOKASSA_AVAILABLE
from moviebot.config import YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY
from moviebot.states import user_payment_state, user_promo_state
from moviebot.utils.promo import apply_promocode, get_promocode_info
from moviebot.utils.payments import create_stars_invoice
from datetime import datetime, timedelta
import pytz

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


def rubles_to_stars(rubles):
    """Конвертирует рубли в Telegram Stars
    80 рублей = 1 доллар = 50 звезд
    Формула: 1 рубль = 50/80 = 0.625 звезды
    Округляет копейки до рублей и звезды до целых значений
    """
    # Округляем рубли до целых (убираем копейки)
    rubles_rounded = round(rubles)
    
    # Конвертируем в звезды: 80 рублей = 50 звезд, значит 1 рубль = 50/80 = 0.625 звезды
    stars = rubles_rounded * 50.0 / 80.0
    
    # Округляем звезды до целых значений (вверх)
    stars_rounded = int(round(stars))
    
    # Минимум 1 звезда, если сумма больше 0
    if stars_rounded == 0 and rubles_rounded > 0:
        stars_rounded = 1
    
    return stars_rounded


# Цены на подписки
SUBSCRIPTION_PRICES = {
    'personal': {
        'notifications': {'month': 100},
        'recommendations': {'month': 100},
        'tickets': {'month': 150},
        'all': {'month': 249, '3months': 599, 'year': 1799, 'lifetime': 2299},
        'test': {'test': 10}  # Тестовый тариф: 10₽, списание раз в 10 минут, только для владельца
    },
    'group': {
        '2': {  # На 2 пользователя (базовый тариф)
            'notifications': {'month': 100},
            'recommendations': {'month': 200},
            'tickets': {'month': 200},
            'all': {'month': 299, '3months': 650, 'year': 1999, 'lifetime': 2500}
        },
        '5': {  # На 5 пользователей (х2 от базового)
            'notifications': {'month': 200},
            'recommendations': {'month': 400},
            'tickets': {'month': 400},
            'all': {'month': 598, '3months': 1300, 'year': 3998, 'lifetime': 5000}
        },
        '10': {  # На 10 пользователей (х3 от базового)
            'notifications': {'month': 300},
            'recommendations': {'month': 600},
            'tickets': {'month': 600},
            'all': {'month': 897, '3months': 1950, 'year': 5997, 'lifetime': 7500}
        }
    }
}


def calculate_discounted_price(user_id, subscription_type, plan_type, period_type, group_size=None):
    """Вычисляет цену с учетом скидок
    
    Логика скидок:
    - Личная не пакетная подписка -> скидка только на не пакетные групповые подписки
    - Личная пакетная подписка -> скидка только на пакетные групповые подписки
    - Групповая не пакетная подписка -> скидка только на не пакетные подписки (личные или групповые)
    - Групповая пакетная подписка -> скидка только на пакетные подписки (личные или групповые)
    """
    # Определяем, является ли plan_type пакетным
    is_package = (plan_type == 'all')
    
    if subscription_type == 'personal':
        base_price = SUBSCRIPTION_PRICES[subscription_type][plan_type].get(period_type, 0)
        
        # Проверяем групповые подписки пользователя
        group_subs = get_user_group_subscriptions(user_id)
        if group_subs:
            for sub in group_subs:
                sub_plan_type = sub.get('plan_type')
                sub_is_package = (sub_plan_type == 'all')
                
                # Скидка только если оба пакетные или оба не пакетные
                if is_package == sub_is_package:
                    # Применяем скидку (20% для группы из 2, 50% для групп из 5 и 10)
                    # Но для личной подписки скидка не зависит от размера группы
                    # Используем фиксированную скидку 20%
                    return int(base_price * 0.8)
        
        return base_price
    
    # Для групповых подписок
    if not group_size:
        group_size = '2'  # По умолчанию 2 пользователя
    
    group_size_str = str(group_size) if isinstance(group_size, int) else group_size
    base_price = SUBSCRIPTION_PRICES[subscription_type][group_size_str][plan_type].get(period_type, 0)
    
    # Проверяем личные подписки пользователя
    personal_subs = get_user_personal_subscriptions(user_id)
    if personal_subs:
        for sub in personal_subs:
            sub_plan_type = sub.get('plan_type')
            sub_is_package = (sub_plan_type == 'all')
            
            # Скидка только если оба пакетные или оба не пакетные
            if is_package == sub_is_package:
                if group_size_str == '2':
                    # Скидка 20% для группы из 2 человек
                    return int(base_price * 0.8)
                elif group_size_str in ['5', '10']:
                    # Скидка 50% для групп из 5 и 10 человек
                    return int(base_price * 0.5)
    
    # Проверяем другие групповые подписки пользователя
    group_subs = get_user_group_subscriptions(user_id)
    if group_subs:
        for sub in group_subs:
            sub_plan_type = sub.get('plan_type')
            sub_is_package = (sub_plan_type == 'all')
            
            # Скидка только если оба пакетные или оба не пакетные
            if is_package == sub_is_package:
                if group_size_str == '2':
                    # Скидка 20% для группы из 2 человек
                    return int(base_price * 0.8)
                elif group_size_str in ['5', '10']:
                    # Скидка 50% для групп из 5 и 10 человек
                    return int(base_price * 0.5)
    
    return base_price


def register_payment_callbacks(bot_instance):
    """Регистрирует callback handlers для платежей"""
    
    @bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("payment:"))
    def handle_payment_callback(call):
        """Обработчик callback для кнопок оплаты"""
        # Явно указываем, что используем глобальные переменные
        global YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY, pytz
        try:
            bot_instance.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            action = call.data.split(":", 1)[1]
            is_private = call.message.chat.type == 'private'
        
            logger.info(f"[PAYMENT CALLBACK] Получен callback от {user_id}, action={action}, is_private={is_private}, chat_id={chat_id}")
        
            from moviebot.database.db_operations import (
                get_active_subscription, get_active_subscription_by_username, 
                get_active_group_subscription, get_user_personal_subscriptions,
                get_user_group_subscriptions, cancel_subscription
            )
        
            if action.startswith("reminder_ok:"):
                # Подтверждение получения напоминания о списании
                try:
                    subscription_id = int(action.split(":")[1])
                    bot_instance.answer_callback_query(call.id, "✅ Напоминание получено")
                    # Удаляем кнопки из сообщения
                    try:
                        bot_instance.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
                    except:
                        pass
                    logger.info(f"[PAYMENT REMINDER] Пользователь {user_id} подтвердил получение напоминания для подписки {subscription_id}")
                except Exception as e:
                    logger.error(f"[PAYMENT REMINDER] Ошибка обработки подтверждения: {e}")
                return
            
            if action.startswith("retry_payment:"):
                # Повторная попытка провести платеж
                try:
                    subscription_id = int(action.split(":")[1])
                    bot_instance.answer_callback_query(call.id, "⏳ Обработка платежа...")
                    
                    # Получаем информацию о подписке
                    from moviebot.database.db_operations import get_subscription_by_id
                    sub = get_subscription_by_id(subscription_id)
                    
                    if not sub:
                        bot_instance.answer_callback_query(call.id, "❌ Подписка не найдена", show_alert=True)
                        return
                    
                    # Проверяем, что подписка принадлежит пользователю
                    if sub.get('user_id') != user_id:
                        bot_instance.answer_callback_query(call.id, "❌ У вас нет доступа к этой подписке", show_alert=True)
                        return
                    
                    payment_method_id = sub.get('payment_method_id')
                    if not payment_method_id:
                        bot_instance.answer_callback_query(call.id, "❌ Сохраненный способ оплаты не найден", show_alert=True)
                        return
                    
                    # Получаем параметры подписки
                    subscription_type = sub.get('subscription_type')
                    plan_type = sub.get('plan_type')
                    period_type = sub.get('period_type')
                    price = float(sub.get('price', 0))
                    chat_id_sub = sub.get('chat_id')
                    telegram_username = sub.get('telegram_username')
                    group_username = sub.get('group_username')
                    group_size = sub.get('group_size')
                    
                    # Создаем безакцептный платеж
                    from moviebot.api.yookassa_api import create_recurring_payment
                    import uuid as uuid_module
                    
                    payment = create_recurring_payment(
                        user_id=user_id,
                        chat_id=chat_id_sub,
                        subscription_type=subscription_type,
                        plan_type=plan_type,
                        period_type=period_type,
                        amount=price,
                        payment_method_id=payment_method_id,
                        group_size=group_size,
                        telegram_username=telegram_username,
                        group_username=group_username
                    )
                    
                    if not payment:
                        bot_instance.answer_callback_query(call.id, "❌ Не удалось создать платеж", show_alert=True)
                        return
                    
                    # Сохраняем платеж в БД
                    payment_id = str(uuid_module.uuid4())
                    from moviebot.database.db_operations import save_payment, update_payment_status, renew_subscription
                    save_payment(
                        payment_id=payment_id,
                        yookassa_payment_id=payment.id,
                        user_id=user_id,
                        chat_id=chat_id_sub,
                        subscription_type=subscription_type,
                        plan_type=plan_type,
                        period_type=period_type,
                        group_size=group_size,
                        amount=price,
                        status=payment.status
                    )
                    
                    # Если платеж успешен, продлеваем подписку и отправляем уведомление
                    if payment.status == 'succeeded':
                        renew_subscription(subscription_id, period_type)
                        update_payment_status(payment_id, 'succeeded', subscription_id)
                        
                        # Отправляем уведомление об успешном платеже
                        from moviebot.scheduler import send_successful_payment_notification
                        send_successful_payment_notification(
                            chat_id=chat_id_sub,
                            subscription_id=subscription_id,
                            subscription_type=subscription_type,
                            plan_type=plan_type,
                            period_type=period_type
                        )
                        
                        # Удаляем кнопки из сообщения
                        try:
                            bot_instance.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
                        except:
                            pass
                        
                        bot_instance.answer_callback_query(call.id, "✅ Платеж успешно проведен!")
                        logger.info(f"[RETRY PAYMENT] Платеж успешно проведен для подписки {subscription_id}")
                    else:
                        bot_instance.answer_callback_query(call.id, f"❌ Платеж не прошел. Статус: {payment.status}", show_alert=True)
                        logger.warning(f"[RETRY PAYMENT] Платеж {payment.id} не успешен, статус: {payment.status}")
                    
                except Exception as e:
                    logger.error(f"[RETRY PAYMENT] Ошибка обработки повторной попытки платежа: {e}", exc_info=True)
                    bot_instance.answer_callback_query(call.id, "❌ Ошибка при обработке платежа", show_alert=True)
                return
            
            if action == "success_ok":
                # Подтверждение получения уведомления об успешном платеже
                try:
                    bot_instance.answer_callback_query(call.id, "✅ Готово")
                    # Удаляем кнопки из сообщения
                    try:
                        bot_instance.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
                    except:
                        pass
                except Exception as e:
                    logger.error(f"[PAYMENT SUCCESS] Ошибка обработки подтверждения: {e}")
                return
            
            if action == "test_10rub":
                # Дополнительная проверка — только ты в личке
                if call.message.chat.type != 'private' or call.from_user.id != 301810276:
                    bot_instance.answer_callback_query(call.id, "❌ Доступ запрещён", show_alert=True)
                    return
                
                logger.info(f"[PAYMENT] Тестовый платёж 10 ₽ запрошен пользователем {user_id}")
                
                try:
                    bot_instance.answer_callback_query(call.id)
                except:
                    pass
                
                # Создаем тестовый платеж 10₽
                # Используем тестовый тариф
                sub_type = 'personal'
                plan_type = 'test'
                period_type = 'test'
                final_price = 10.0
                
                # Инициализируем ЮKassa
                if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
                    logger.error(f"[PAYMENT] YooKassa ключи не настроены!")
                    bot_instance.answer_callback_query(call.id, "Ошибка: ключи оплаты не настроены", show_alert=True)
                    return
                
                from yookassa import Configuration, Payment
                shop_id = YOOKASSA_SHOP_ID.strip() if YOOKASSA_SHOP_ID else None
                secret_key = YOOKASSA_SECRET_KEY.strip() if YOOKASSA_SECRET_KEY else None
                Configuration.account_id = shop_id
                Configuration.secret_key = secret_key
                
                # Создаем уникальный ID платежа
                import uuid as uuid_module
                payment_id = str(uuid_module.uuid4())
                
                # Определяем URL для возврата
                return_url = os.getenv('YOOKASSA_RETURN_URL', 'tg://resolve?domain=movie_planner_bot')
                
                # Подготавливаем metadata для платежа
                metadata = {
                    "user_id": str(user_id),
                    "chat_id": str(chat_id),
                    "subscription_type": sub_type,
                    "plan_type": plan_type,
                    "period_type": period_type,
                    "payment_id": payment_id,
                    "telegram_username": call.from_user.username or ""
                }
                
                # Создаем платеж через ЮKassa
                try:
                    payment = create_subscription_payment(
                        user_id=user_id,
                        chat_id=chat_id,
                        subscription_type=sub_type,
                        plan_type=plan_type,
                        period_type=period_type,
                        amount=final_price,
                        return_url=return_url,
                        metadata=metadata,
                        group_size=None,
                        telegram_username=call.from_user.username,
                        group_username=None
                    )
                    
                    if not payment:
                        logger.error(f"[PAYMENT] Не удалось создать тестовый платеж для пользователя {user_id}")
                        bot_instance.answer_callback_query(call.id, "❌ Ошибка создания платежа", show_alert=True)
                        return
                    
                    # Сохраняем платеж в БД
                    from moviebot.database.db_operations import save_payment
                    save_payment(
                        payment_id=payment_id,
                        yookassa_payment_id=payment.id,
                        user_id=user_id,
                        chat_id=chat_id,
                        subscription_type=sub_type,
                        plan_type=plan_type,
                        period_type=period_type,
                        group_size=None,
                        amount=final_price,
                        status=payment.status
                    )
                    
                    # Отправляем ссылку на оплату
                    payment_url = payment.confirmation.confirmation_url if payment.confirmation else None
                    if payment_url:
                        text = f"🧪 <b>Тестовый платёж 10 ₽</b>\n\n"
                        text += f"Ссылка на оплату:\n{payment_url}"
                        markup = InlineKeyboardMarkup()
                        markup.add(InlineKeyboardButton("💳 Оплатить", url=payment_url))
                        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:tariffs:personal"))
                        
                        try:
                            bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                        except Exception as e:
                            if "message is not modified" not in str(e):
                                logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                                bot_instance.send_message(call.message.chat.id, text, reply_markup=markup, parse_mode='HTML')
                    else:
                        logger.error(f"[PAYMENT] Платеж создан, но нет ссылки на оплату")
                        bot_instance.answer_callback_query(call.id, "❌ Ошибка: нет ссылки на оплату", show_alert=True)
                    
                    logger.info(f"[PAYMENT] Тестовый платёж 10 ₽ создан для пользователя {user_id}, payment_id={payment_id}")
                    
                except Exception as e:
                    logger.error(f"[PAYMENT] Ошибка создания тестового платежа: {e}", exc_info=True)
                    bot_instance.answer_callback_query(call.id, "❌ Ошибка создания платежа", show_alert=True)
                return
        
            if action == "active":
                # Показываем действующие подписки
                markup = InlineKeyboardMarkup(row_width=1)
                
                # В групповом чате скрываем кнопку "Личная подписка"
                if is_private:
                    markup.add(InlineKeyboardButton("👤 Личная подписка", callback_data="payment:active:personal"))
                    text = "📋 <b>Действующая подписка</b>\n\nВыберите тип подписки:"
                else:
                    # В групповом чате показываем только групповую подписку
                    text = "📋 <b>Действующая подписка</b>\n\n"
                    text += "💡 <i>Личную подписку можно посмотреть в личных сообщениях бота</i>\n\n"
                    text += "Выберите тип подписки:"
                
                markup.add(InlineKeyboardButton("👥 Групповая подписка", callback_data="payment:active:group"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:back"))
            
                try:
                    bot_instance.edit_message_text(
                        text,
                        call.message.chat.id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action.startswith("active:personal"):
                # Проверка личной подписки
                if is_private:
                    # В личке - проверяем все активные подписки пользователя
                    from moviebot.database.db_operations import get_user_personal_subscriptions
                    all_subs = get_user_personal_subscriptions(user_id)
                    
                    # Фильтруем только активные подписки
                    active_subs = []
                    seen_plan_types = set()
                    now = datetime.now(pytz.UTC)
                    total_price = 0
                    
                    for sub in all_subs:
                        expires_at = sub.get('expires_at')
                        plan_type = sub.get('plan_type')
                        
                        # Проверяем, что подписка активна
                        is_active = False
                        if not expires_at:
                            is_active = True
                        elif isinstance(expires_at, datetime):
                            if expires_at.tzinfo is None:
                                expires_at = pytz.UTC.localize(expires_at)
                            if expires_at.tzinfo != pytz.UTC:
                                expires_at = expires_at.astimezone(pytz.UTC)
                            is_active = expires_at > now
                        else:
                            try:
                                if isinstance(expires_at, str):
                                    expires_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                                    if expires_dt.tzinfo is None:
                                        expires_dt = pytz.UTC.localize(expires_dt)
                                    if expires_dt.tzinfo != pytz.UTC:
                                        expires_dt = expires_dt.astimezone(pytz.UTC)
                                    is_active = expires_dt > now
                                else:
                                    is_active = True
                            except:
                                is_active = True
                        
                        # Добавляем только активные и уникальные по plan_type
                        if is_active and plan_type and plan_type not in seen_plan_types:
                            active_subs.append(sub)
                            seen_plan_types.add(plan_type)
                            total_price += sub.get('price', 0)
                    
                    if active_subs:
                        # Используем первую подписку для получения общей информации
                        sub = active_subs[0]
                        expires_at = sub.get('expires_at')
                        next_payment = sub.get('next_payment_date')
                        activated = sub.get('activated_at')
                        plan_type = sub.get('plan_type', 'all')
                        period_type = sub.get('period_type', 'lifetime')
                        
                        # Определяем названия подписок
                        plan_names = {
                            'notifications': 'Уведомления о сериалах',
                            'recommendations': 'Рекомендации',
                            'tickets': 'Билеты',
                            'all': 'Все режимы'
                        }
                        
                        # Формируем список названий подписок
                        if len(active_subs) == 1:
                            plan_name = plan_names.get(plan_type, plan_type)
                            text = f"👤 <b>Личная подписка</b>\n\n"
                            text += f"📋 <b>Название подписки:</b> {plan_name}\n\n"
                        else:
                            text = f"👤 <b>Личная подписка</b>\n\n"
                            text += f"📋 <b>Активные подписки:</b>\n"
                            for active_sub in active_subs:
                                sub_plan_type = active_sub.get('plan_type', 'all')
                                sub_plan_name = plan_names.get(sub_plan_type, sub_plan_type)
                                sub_price = active_sub.get('price', 0)
                                text += f"• {sub_plan_name} ({sub_price}₽)\n"
                            text += "\n"
                        
                        text += f"💰 <b>Общая сумма платежа: {total_price}₽</b>\n"
                        if activated:
                            text += f"📅 Дата активации: <b>{activated.strftime('%d.%m.%Y') if isinstance(activated, datetime) else activated}</b>\n"
                        if next_payment:
                            text += f"📅 Следующее списание: <b>{next_payment.strftime('%d.%m.%Y') if isinstance(next_payment, datetime) else next_payment}</b>\n"
                        if expires_at:
                            text += f"⏰ Действует до: <b>{expires_at.strftime('%d.%m.%Y') if isinstance(expires_at, datetime) else expires_at}</b>\n"
                        else:
                            text += f"⏰ Действует: <b>Навсегда</b>\n"
                        
                        markup = InlineKeyboardMarkup(row_width=1)
                        
                        # Если несколько подписок, показываем кнопку "Изменить подписку" и кнопки "Отменить" для каждой
                        if len(active_subs) > 1:
                            markup.add(InlineKeyboardButton("✏️ Изменить подписку", callback_data="payment:modify:all"))
                            for active_sub in active_subs:
                                sub_id = active_sub.get('id')
                                if sub_id and sub_id > 0:
                                    sub_plan_type = active_sub.get('plan_type', 'all')
                                    sub_plan_name = plan_names.get(sub_plan_type, sub_plan_type)
                                    markup.add(InlineKeyboardButton(f"❌ Отменить: {sub_plan_name}", callback_data=f"payment:cancel:{sub_id}"))
                        else:
                            # Одна подписка - показываем стандартные кнопки
                            subscription_id = sub.get('id')
                            if subscription_id is None:
                                subscription_id = 0
                            
                            if subscription_id and subscription_id > 0:
                                markup.add(InlineKeyboardButton("✏️ Изменить подписку", callback_data=f"payment:modify:{subscription_id}"))
                                markup.add(InlineKeyboardButton("❌ Отменить", callback_data=f"payment:cancel:{subscription_id}"))
                            else:
                                markup.add(InlineKeyboardButton("✏️ Изменить подписку", callback_data="payment:tariffs:personal"))
                                markup.add(InlineKeyboardButton("❌ Отменить", callback_data="payment:cancel:personal"))
                        
                        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active"))
                        try:
                            bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                        except Exception as e:
                            if "message is not modified" not in str(e):
                                logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                        return
                    else:
                        text = "👤 <b>Личная подписка</b>\n\n"
                        text += "❌ Активная подписка отсутствует, выберите тариф для подключения"
                        markup = InlineKeyboardMarkup(row_width=1)
                        markup.add(InlineKeyboardButton("💰 Тарифы", callback_data="payment:tariffs:personal"))
                        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active"))
                        try:
                            bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                        except Exception as e:
                            if "message is not modified" not in str(e):
                                logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                        return
                else:
                    # В группе - показываем все активные подписки инициатора
                    from moviebot.database.db_operations import get_user_personal_subscriptions
                    all_subs = get_user_personal_subscriptions(user_id)
                    
                    # Фильтруем только активные подписки
                    active_subs = []
                    seen_plan_types = set()
                    now = datetime.now(pytz.UTC)
                    total_price = 0
                    
                    for sub in all_subs:
                        expires_at = sub.get('expires_at')
                        plan_type = sub.get('plan_type')
                        
                        # Проверяем, что подписка активна
                        is_active = False
                        if not expires_at:
                            is_active = True
                        elif isinstance(expires_at, datetime):
                            if expires_at.tzinfo is None:
                                expires_at = pytz.UTC.localize(expires_at)
                            if expires_at.tzinfo != pytz.UTC:
                                expires_at = expires_at.astimezone(pytz.UTC)
                            is_active = expires_at > now
                        else:
                            try:
                                if isinstance(expires_at, str):
                                    expires_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                                    if expires_dt.tzinfo is None:
                                        expires_dt = pytz.UTC.localize(expires_dt)
                                    if expires_dt.tzinfo != pytz.UTC:
                                        expires_dt = expires_dt.astimezone(pytz.UTC)
                                    is_active = expires_dt > now
                                else:
                                    is_active = True
                            except:
                                is_active = True
                        
                        # Добавляем только активные и уникальные по plan_type
                        if is_active and plan_type and plan_type not in seen_plan_types:
                            active_subs.append(sub)
                            seen_plan_types.add(plan_type)
                            total_price += sub.get('price', 0)
                    
                    if active_subs:
                        # Используем первую подписку для получения общей информации
                        sub = active_subs[0]
                        expires_at = sub.get('expires_at')
                        next_payment = sub.get('next_payment_date')
                        activated = sub.get('activated_at')
                        plan_type = sub.get('plan_type', 'all')
                        period_type = sub.get('period_type', 'lifetime')
                        
                        # Определяем названия подписок
                        plan_names = {
                            'notifications': 'Уведомления о сериалах',
                            'recommendations': 'Рекомендации',
                            'tickets': 'Билеты',
                            'all': 'Все режимы'
                        }
                        
                        # Формируем список названий подписок
                        if len(active_subs) == 1:
                            plan_name = plan_names.get(plan_type, plan_type)
                            text = f"👤 <b>Личная подписка</b>\n\n"
                            text += f"📋 <b>Название подписки:</b> {plan_name}\n\n"
                        else:
                            text = f"👤 <b>Личная подписка</b>\n\n"
                            text += f"📋 <b>Активные подписки:</b>\n"
                            for active_sub in active_subs:
                                sub_plan_type = active_sub.get('plan_type', 'all')
                                sub_plan_name = plan_names.get(sub_plan_type, sub_plan_type)
                                sub_price = active_sub.get('price', 0)
                                text += f"• {sub_plan_name} ({sub_price}₽)\n"
                            text += "\n"
                        
                        text += f"Пользователь: <b>@{call.from_user.username or f'user_{user_id}'}</b>\n"
                        text += f"💰 <b>Общая сумма платежа: {total_price}₽</b>\n"
                        if activated:
                            text += f"📅 Дата активации: <b>{activated.strftime('%d.%m.%Y') if isinstance(activated, datetime) else activated}</b>\n"
                        if next_payment:
                            text += f"📅 Следующее списание: <b>{next_payment.strftime('%d.%m.%Y') if isinstance(next_payment, datetime) else next_payment}</b>\n"
                        if expires_at:
                            text += f"⏰ Действует до: <b>{expires_at.strftime('%d.%m.%Y') if isinstance(expires_at, datetime) else expires_at}</b>\n"
                        else:
                            text += f"⏰ Действует: <b>Навсегда</b>\n"
                        
                        markup = InlineKeyboardMarkup(row_width=1)
                        
                        # Если несколько подписок, показываем кнопку "Изменить подписку" и кнопки "Отменить" для каждой
                        if len(active_subs) > 1:
                            markup.add(InlineKeyboardButton("✏️ Изменить подписку", callback_data="payment:modify:all"))
                            for active_sub in active_subs:
                                sub_id = active_sub.get('id')
                                if sub_id and sub_id > 0:
                                    sub_plan_type = active_sub.get('plan_type', 'all')
                                    sub_plan_name = plan_names.get(sub_plan_type, sub_plan_type)
                                    markup.add(InlineKeyboardButton(f"❌ Отменить: {sub_plan_name}", callback_data=f"payment:cancel:{sub_id}"))
                        else:
                            # Одна подписка - показываем стандартные кнопки
                            subscription_id = sub.get('id')
                            if subscription_id is None:
                                subscription_id = 0
                            
                            if subscription_id and subscription_id > 0:
                                markup.add(InlineKeyboardButton("✏️ Изменить подписку", callback_data=f"payment:modify:{subscription_id}"))
                                markup.add(InlineKeyboardButton("❌ Отменить", callback_data=f"payment:cancel:{subscription_id}"))
                            else:
                                markup.add(InlineKeyboardButton("✏️ Изменить подписку", callback_data="payment:tariffs:personal"))
                                markup.add(InlineKeyboardButton("❌ Отменить", callback_data="payment:cancel:personal"))
                        
                        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active"))
                    try:
                            bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    except Exception as e:
                        if "message is not modified" not in str(e):
                            logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                    else:
                        text = "👤 <b>Личная подписка</b>\n\n"
                        text += "❌ Активная подписка отсутствует, выберите тариф для подключения"
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("💰 Тарифы", callback_data="payment:tariffs:personal"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active"))
                    
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            # Сначала проверяем точные совпадения для групповых подписок
            if action == "active:group:current":
                # Проверка подписки текущей группы
                try:
                    bot_instance.answer_callback_query(call.id)
                except:
                    pass
            
                from moviebot.database.db_operations import get_subscription_members, get_active_group_users, get_user_group_subscriptions
                
                # Если пользователь в личном чате, получаем chat_id группы из подписки
                if is_private:
                    # Получаем групповые подписки пользователя
                    group_subs = get_user_group_subscriptions(user_id)
                    if group_subs:
                        # Берем первую активную групповую подписку
                        sub = group_subs[0]
                        chat_id = sub.get('chat_id', chat_id)  # Используем chat_id из подписки
                        logger.info(f"[PAYMENT] Пользователь в личном чате, используем chat_id из подписки: {chat_id}")
                    else:
                        sub = None
                else:
                    # Если пользователь в группе, используем chat_id группы
                    sub = get_active_subscription(chat_id, user_id, 'group')
            
                logger.info(f"[PAYMENT] Проверка подписки для группы {chat_id}, user_id={user_id}, sub={sub}")
            
                # Не создаем виртуальную подписку автоматически
                # Если подписки нет, показываем сообщение об отсутствии подписки
            
                if sub:
                    expires_at = sub.get('expires_at')
                    next_payment = sub.get('next_payment_date')
                    price = sub.get('price', 0)
                    activated = sub.get('activated_at')
                    group_size = sub.get('group_size')
                    subscription_id = sub.get('id')
                    plan_type = sub.get('plan_type', 'all')
                    period_type = sub.get('period_type', 'lifetime')
                
                    # Получаем информацию о группе
                    try:
                        chat = bot_instance.get_chat(chat_id)
                        group_title = chat.title
                        group_username = chat.username
                    except Exception as chat_error:
                        logger.error(f"[PAYMENT] Ошибка получения информации о группе: {chat_error}")
                        group_title = "Группа"
                        group_username = None
                    
                    text = f"👥 <b>Групповая подписка</b>\n\n"
                    if plan_type == 'all':
                        text += f"📦 <b>Пакетная подписка - Все режимы</b>\n\n"
                    text += f"Группа: <b>{group_title}</b>\n"
                    if group_username:
                        text += f"@{group_username}\n"
                    text += f"\n💰 Сумма платежа: <b>{price}₽</b>\n"
                    if group_size:
                        text += f"👥 Количество участников: <b>{group_size}</b>\n"
                        if subscription_id and subscription_id > 0:
                            try:
                                members = get_subscription_members(subscription_id)
                                # Исключаем бота из списка участников
                                if BOT_ID and BOT_ID in members:
                                    members = {uid: uname for uid, uname in members.items() if uid != BOT_ID}
                                members_count = len(members) if members else 0
                                text += f"✅ Участников в подписке: <b>{members_count}</b>\n"
                            except Exception as members_error:
                                logger.error(f"[PAYMENT] Ошибка получения участников подписки: {members_error}")
                                # Пытаемся получить количество из активных пользователей группы
                                try:
                                    active_users = get_active_group_users(chat_id, bot_id=BOT_ID)
                                    if active_users and BOT_ID:
                                        active_users = {uid: uname for uid, uname in active_users.items() if uid != BOT_ID}
                                    active_count = len(active_users) if active_users else 0
                                    text += f"✅ Участников в подписке: <b>{active_count}</b>\n"
                                except Exception as active_error:
                                    logger.error(f"[PAYMENT] Ошибка получения активных пользователей: {active_error}")
                                    text += f"✅ Участников в подписке: <b>?</b>\n"
                    if activated:
                        text += f"📅 Дата активации: <b>{activated.strftime('%d.%m.%Y') if isinstance(activated, datetime) else activated}</b>\n"
                    if next_payment:
                        text += f"📅 Следующее списание: <b>{next_payment.strftime('%d.%m.%Y') if isinstance(next_payment, datetime) else next_payment}</b>\n"
                    if expires_at:
                        text += f"⏰ Действует до: <b>{expires_at.strftime('%d.%m.%Y') if isinstance(expires_at, datetime) else expires_at}</b>\n"
                    else:
                        text += f"⏰ Действует: <b>Навсегда</b>\n"
                
                    markup = InlineKeyboardMarkup(row_width=1)
                    # Показываем кнопку списка участников только для реальных подписок (не виртуальных)
                    if subscription_id and subscription_id > 0:
                        markup.add(InlineKeyboardButton("👥 Список участников", callback_data=f"payment:group_members:{subscription_id}"))
                
                    # Предложение других функций, если подписка не включает все режимы
                    if subscription_id and subscription_id > 0 and plan_type != 'all':
                        # Вычисляем цены для добавления других функций
                        group_size_str = str(group_size) if group_size else '2'
                        # Для отдельных функций доступна только месячная подписка
                        current_price = SUBSCRIPTION_PRICES['group'][group_size_str][plan_type].get('month', 0) if period_type == 'month' else SUBSCRIPTION_PRICES['group'][group_size_str][plan_type].get(period_type, 0)
                        all_price = SUBSCRIPTION_PRICES['group'][group_size_str]['all'].get(period_type, 0)
                    
                        # Определяем, какие функции отсутствуют
                        missing_functions = []
                        if plan_type != 'notifications':
                            missing_functions.append(('notifications', '🔔 Уведомления', SUBSCRIPTION_PRICES['group'][group_size_str]['notifications'].get('month', 0)))
                        if plan_type != 'recommendations':
                            missing_functions.append(('recommendations', '🎯 Рекомендации', SUBSCRIPTION_PRICES['group'][group_size_str]['recommendations'].get('month', 0)))
                        if plan_type != 'tickets':
                            missing_functions.append(('tickets', '🎫 Билеты', SUBSCRIPTION_PRICES['group'][group_size_str]['tickets'].get('month', 0)))
                    
                        # Предлагаем добавить недостающие функции или обновить до "Все режимы"
                        if missing_functions:
                            # Предлагаем обновить до "Все режимы" (обычно выгоднее)
                            # Для расчета используем месячную цену, так как отдельные функции доступны только по месячной подписке
                            current_month_price = SUBSCRIPTION_PRICES['group'][group_size_str][plan_type].get('month', 0)
                            all_month_price = SUBSCRIPTION_PRICES['group'][group_size_str]['all'].get('month', 0)
                            upgrade_price = all_month_price - current_month_price
                            if upgrade_price > 0:
                                markup.add(InlineKeyboardButton(f"📦 Все режимы (+{upgrade_price}₽/мес)", callback_data=f"payment:upgrade_plan:{subscription_id}:all"))
                        
                            # Предлагаем добавить отдельные функции (если их 1-2)
                            if len(missing_functions) <= 2:
                                for func_type, func_name, func_price in missing_functions:
                                    # Для отдельных функций всегда месячная подписка
                                    add_price = func_price - current_month_price if func_price > current_month_price else func_price
                                    if add_price > 0:
                                        markup.add(InlineKeyboardButton(f"{func_name} (+{add_price}₽/мес)", callback_data=f"payment:upgrade_plan:{subscription_id}:{func_type}"))
                
                    # Кнопки расширения подписки (только для реальных подписок с ограничением по участникам)
                    if subscription_id and subscription_id > 0 and (group_size is None or group_size == 2):
                        # Можно расширить до 5 или 10
                        plan_type = sub.get('plan_type')
                        period_type = sub.get('period_type')
                        current_price = SUBSCRIPTION_PRICES['group']['2'][plan_type].get(period_type, 0)
                        price_5 = SUBSCRIPTION_PRICES['group']['5'][plan_type].get(period_type, 0)
                        price_10 = SUBSCRIPTION_PRICES['group']['10'][plan_type].get(period_type, 0)
                        diff_5 = price_5 - current_price
                        diff_10 = price_10 - current_price
                    
                        # Применяем скидку, если есть личная подписка
                        from moviebot.database.db_operations import get_user_personal_subscriptions
                        personal_subs = get_user_personal_subscriptions(user_id)
                        if personal_subs:
                            diff_5 = int(diff_5 * 0.5)  # Скидка 50%
                            diff_10 = int(price_10 * 0.5) - current_price
                    
                        markup.add(InlineKeyboardButton(f"📈 Расширить до 5 (+{diff_5}₽)", callback_data=f"payment:expand:5:{subscription_id}"))
                        markup.add(InlineKeyboardButton(f"📈 Расширить до 10 (+{diff_10}₽)", callback_data=f"payment:expand:10:{subscription_id}"))
                    elif subscription_id and subscription_id > 0 and group_size == 5:
                        # Можно расширить только до 10
                        plan_type = sub.get('plan_type')
                        period_type = sub.get('period_type')
                        current_price = SUBSCRIPTION_PRICES['group']['5'][plan_type].get(period_type, 0)
                        price_10 = SUBSCRIPTION_PRICES['group']['10'][plan_type].get(period_type, 0)
                        diff_10 = price_10 - current_price
                    
                        # Применяем скидку, если есть личная подписка
                        from moviebot.database.db_operations import get_user_personal_subscriptions
                        personal_subs = get_user_personal_subscriptions(user_id)
                        if personal_subs:
                            diff_10 = int(price_10 * 0.5) - current_price
                    
                        markup.add(InlineKeyboardButton(f"📈 Расширить до 10 (+{diff_10}₽)", callback_data=f"payment:expand:10:{subscription_id}"))
                
                    # Показываем кнопку "Отписаться" только для реальных подписок (id > 0) и только для активных участников
                    if subscription_id and subscription_id > 0:
                        from moviebot.database.db_operations import get_subscription_members
                        members = get_subscription_members(subscription_id)
                        # Исключаем бота из списка участников
                        if BOT_ID and BOT_ID in members:
                            members = {uid: uname for uid, uname in members.items() if uid != BOT_ID}
                        # Проверяем, является ли пользователь активным участником подписки
                        if members and user_id in members:
                            markup.add(InlineKeyboardButton("❌ Отписаться", callback_data=f"payment:cancel:{subscription_id}"))
                
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group"))
                else:
                    text = "👥 <b>Групповая подписка</b>\n\n"
                    text += "❌ Активная подписка отсутствует, выберите тариф для подключения"
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("💰 Тарифы", callback_data="payment:tariffs:group"))
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group"))
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action.startswith("group_members:"):
                # Показываем список участников подписки с пагинацией
                parts = action.split(":")
                subscription_id = int(parts[1])
                page = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
                
                from moviebot.database.db_operations import get_subscription_members, get_active_group_users, get_subscription_by_id
                
                # Получаем информацию о подписке для определения chat_id и group_size
                sub = get_subscription_by_id(subscription_id)
                if not sub:
                    bot_instance.answer_callback_query(call.id, "Подписка не найдена", show_alert=True)
                    return
                
                group_chat_id = sub.get('chat_id')
                group_size = sub.get('group_size')
                
                # Если в личке, используем chat_id из подписки
                if is_private:
                    chat_id = group_chat_id
                
                members = get_subscription_members(subscription_id)
                # Исключаем бота из списка участников
                if BOT_ID and BOT_ID in members:
                    members = {uid: uname for uid, uname in members.items() if uid != BOT_ID}
                
                active_users = get_active_group_users(chat_id, bot_id=BOT_ID)
                # Исключаем бота из списка активных пользователей
                if active_users and BOT_ID:
                    active_users = {uid: uname for uid, uname in active_users.items() if uid != BOT_ID}
                
                # Пагинация: 10 пользователей на страницу
                items_per_page = 10
                active_users_list = list(active_users.items())
                total_users = len(active_users_list)
                total_pages = (total_users + items_per_page - 1) // items_per_page
                start_idx = page * items_per_page
                end_idx = min(start_idx + items_per_page, total_users)
                
                text = "👥 <b>Список участников</b>\n\n"
                text += "💸 - участник в подписке\n\n"
            
                if active_users_list:
                    for user_id_member, username in active_users_list[start_idx:end_idx]:
                        is_member = user_id_member in members
                        emoji = "💸" if is_member else "⬜"
                        text += f"{emoji} @{username}\n"
                    
                    if total_pages > 1:
                        text += f"\nСтраница {page + 1} из {total_pages}"
                else:
                    text += "Нет активных участников"
            
                markup = InlineKeyboardMarkup(row_width=1)
                
                # Кнопки добавления участников (если есть места)
                members_count = len(members) if members else 0
                if group_size and members_count < group_size:
                    # Есть места для добавления участников
                    # Находим пользователей, которые не в подписке, для текущей страницы
                    not_in_subscription = []
                    for user_id_member, username in active_users_list[start_idx:end_idx]:
                        if user_id_member not in members:
                            not_in_subscription.append((user_id_member, username))
                    
                    # Добавляем кнопки для добавления участников на текущей странице
                    for user_id_member, username in not_in_subscription:
                        button_text = f"➕ @{username}"
                        if len(button_text) > 50:
                            button_text = button_text[:47] + "..."
                        markup.add(InlineKeyboardButton(
                            button_text,
                            callback_data=f"payment:add_member:{subscription_id}:{user_id_member}"
                        ))
                
                elif group_size and members_count >= group_size:
                    # Места закончились - показываем кнопки расширения
                    current_size = group_size
                    plan_type_sub = sub.get('plan_type')
                    period_type_sub = sub.get('period_type')
                    
                    if current_size == 2:
                        # Можно расширить до 5 или 10
                        current_price = SUBSCRIPTION_PRICES['group']['2'][plan_type_sub].get(period_type_sub, 0)
                        price_5 = SUBSCRIPTION_PRICES['group']['5'][plan_type_sub].get(period_type_sub, 0)
                        price_10 = SUBSCRIPTION_PRICES['group']['10'][plan_type_sub].get(period_type_sub, 0)
                        diff_5 = price_5 - current_price
                        diff_10 = price_10 - current_price
                        
                        from moviebot.database.db_operations import get_user_personal_subscriptions
                        personal_subs = get_user_personal_subscriptions(user_id)
                        if personal_subs:
                            diff_5 = int(diff_5 * 0.5)
                            diff_10 = int(price_10 * 0.5) - current_price
                        
                        markup.add(InlineKeyboardButton(f"📈 Расширить до 5 (+{diff_5}₽)", callback_data=f"payment:expand:5:{subscription_id}"))
                        markup.add(InlineKeyboardButton(f"📈 Расширить до 10 (+{diff_10}₽)", callback_data=f"payment:expand:10:{subscription_id}"))
                    elif current_size == 5:
                        # Можно расширить до 10
                        current_price = SUBSCRIPTION_PRICES['group']['5'][plan_type_sub].get(period_type_sub, 0)
                        price_10 = SUBSCRIPTION_PRICES['group']['10'][plan_type_sub].get(period_type_sub, 0)
                        diff_10 = price_10 - current_price
                        
                        from moviebot.database.db_operations import get_user_personal_subscriptions
                        personal_subs = get_user_personal_subscriptions(user_id)
                        if personal_subs:
                            diff_10 = int(price_10 * 0.5) - current_price
                        
                        markup.add(InlineKeyboardButton(f"📈 Расширить до 10 (+{diff_10}₽)", callback_data=f"payment:expand:10:{subscription_id}"))
                
                # Кнопки пагинации
                nav_buttons = []
                if page > 0:
                    nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:group_members:{subscription_id}:{page-1}"))
                if page < total_pages - 1:
                    nav_buttons.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"payment:group_members:{subscription_id}:{page+1}"))
                
                if nav_buttons:
                    markup.add(*nav_buttons)
                
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group:current"))
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
            
            if action.startswith("add_member:"):
                # Добавление участника в подписку после оплаты
                parts = action.split(":")
                if len(parts) < 3:
                    bot_instance.answer_callback_query(call.id, "❌ Ошибка: неверный формат", show_alert=True)
                    return
                
                subscription_id = int(parts[1])
                target_user_id = int(parts[2])
                
                from moviebot.database.db_operations import (
                    get_subscription_by_id, add_subscription_member,
                    get_subscription_members, get_active_group_users
                )
                
                # Получаем информацию о подписке
                sub = get_subscription_by_id(subscription_id)
                if not sub:
                    bot_instance.answer_callback_query(call.id, "❌ Подписка не найдена", show_alert=True)
                    return
                
                group_chat_id = sub.get('chat_id')
                group_size = sub.get('group_size')
                
                # Если в личке, используем chat_id из подписки
                if is_private:
                    chat_id = group_chat_id
                
                # Проверяем, что целевой пользователь в группе
                active_users = get_active_group_users(chat_id, bot_id=BOT_ID)
                if target_user_id not in active_users:
                    bot_instance.answer_callback_query(call.id, "❌ Пользователь не найден в группе", show_alert=True)
                    return
                
                # Проверяем, что пользователь еще не в подписке
                members = get_subscription_members(subscription_id)
                if BOT_ID and BOT_ID in members:
                    members = {uid: uname for uid, uname in members.items() if uid != BOT_ID}
                if target_user_id in members:
                    bot_instance.answer_callback_query(call.id, "✅ Этот пользователь уже в подписке")
                    return
                
                # Проверяем лимит участников
                if group_size and len(members) >= group_size:
                    bot_instance.answer_callback_query(call.id, f"❌ Достигнут лимит участников ({group_size})", show_alert=True)
                    return
                
                # Добавляем участника
                target_username = active_users.get(target_user_id, f"user_{target_user_id}")
                add_subscription_member(subscription_id, target_user_id, target_username)
                
                bot_instance.answer_callback_query(call.id, f"✅ @{target_username} добавлен в подписку")
                
                # Возвращаемся к списку участников (первая страница)
                try:
                    # Получаем обновленный список участников
                    members = get_subscription_members(subscription_id)
                    if BOT_ID and BOT_ID in members:
                        members = {uid: uname for uid, uname in members.items() if uid != BOT_ID}
                    
                    active_users = get_active_group_users(chat_id, bot_id=BOT_ID)
                    if active_users and BOT_ID:
                        active_users = {uid: uname for uid, uname in active_users.items() if uid != BOT_ID}
                    
                    # Пагинация: 10 пользователей на страницу
                    items_per_page = 10
                    active_users_list = list(active_users.items())
                    total_users = len(active_users_list)
                    page = 0  # Возвращаемся на первую страницу
                    total_pages = (total_users + items_per_page - 1) // items_per_page
                    start_idx = page * items_per_page
                    end_idx = min(start_idx + items_per_page, total_users)
                    
                    text = "👥 <b>Список участников</b>\n\n"
                    text += "💸 - участник в подписке\n\n"
                
                    if active_users_list:
                        for user_id_member, username in active_users_list[start_idx:end_idx]:
                            is_member = user_id_member in members
                            emoji = "💸" if is_member else "⬜"
                            text += f"{emoji} @{username}\n"
                        
                        if total_pages > 1:
                            text += f"\nСтраница {page + 1} из {total_pages}"
                    else:
                        text += "Нет активных участников"
                
                    markup = InlineKeyboardMarkup(row_width=1)
                    
                    # Кнопки добавления участников (если есть места)
                    members_count = len(members) if members else 0
                    if group_size and members_count < group_size:
                        # Есть места для добавления участников
                        not_in_subscription = []
                        for user_id_member, username in active_users_list[start_idx:end_idx]:
                            if user_id_member not in members:
                                not_in_subscription.append((user_id_member, username))
                        
                        # Добавляем кнопки для добавления участников на текущей странице
                        for user_id_member, username in not_in_subscription:
                            button_text = f"➕ @{username}"
                            if len(button_text) > 50:
                                button_text = button_text[:47] + "..."
                            markup.add(InlineKeyboardButton(
                                button_text,
                                callback_data=f"payment:add_member:{subscription_id}:{user_id_member}"
                            ))
                    
                    elif group_size and members_count >= group_size:
                        # Места закончились - показываем кнопки расширения
                        current_size = group_size
                        plan_type_sub = sub.get('plan_type')
                        period_type_sub = sub.get('period_type')
                        
                        if current_size == 2:
                            current_price = SUBSCRIPTION_PRICES['group']['2'][plan_type_sub].get(period_type_sub, 0)
                            price_5 = SUBSCRIPTION_PRICES['group']['5'][plan_type_sub].get(period_type_sub, 0)
                            price_10 = SUBSCRIPTION_PRICES['group']['10'][plan_type_sub].get(period_type_sub, 0)
                            diff_5 = price_5 - current_price
                            diff_10 = price_10 - current_price
                            
                            from moviebot.database.db_operations import get_user_personal_subscriptions
                            personal_subs = get_user_personal_subscriptions(user_id)
                            if personal_subs:
                                diff_5 = int(diff_5 * 0.5)
                                diff_10 = int(price_10 * 0.5) - current_price
                            
                            markup.add(InlineKeyboardButton(f"📈 Расширить до 5 (+{diff_5}₽)", callback_data=f"payment:expand:5:{subscription_id}"))
                            markup.add(InlineKeyboardButton(f"📈 Расширить до 10 (+{diff_10}₽)", callback_data=f"payment:expand:10:{subscription_id}"))
                        elif current_size == 5:
                            current_price = SUBSCRIPTION_PRICES['group']['5'][plan_type_sub].get(period_type_sub, 0)
                            price_10 = SUBSCRIPTION_PRICES['group']['10'][plan_type_sub].get(period_type_sub, 0)
                            diff_10 = price_10 - current_price
                            
                            from moviebot.database.db_operations import get_user_personal_subscriptions
                            personal_subs = get_user_personal_subscriptions(user_id)
                            if personal_subs:
                                diff_10 = int(price_10 * 0.5) - current_price
                            
                            markup.add(InlineKeyboardButton(f"📈 Расширить до 10 (+{diff_10}₽)", callback_data=f"payment:expand:10:{subscription_id}"))
                    
                    # Кнопки пагинации
                    nav_buttons = []
                    if page > 0:
                        nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:group_members:{subscription_id}:{page-1}"))
                    if page < total_pages - 1:
                        nav_buttons.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"payment:group_members:{subscription_id}:{page+1}"))
                    
                    if nav_buttons:
                        markup.add(*nav_buttons)
                    
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group:current"))
                    
                    bot_instance.edit_message_text(
                        text,
                        call.message.chat.id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logger.error(f"[PAYMENT ADD MEMBER] Ошибка обновления сообщения: {e}", exc_info=True)
                return
            
            if action == "success_ok":
                # Закрываем сообщение об успешной оплате
                try:
                    bot_instance.delete_message(call.message.chat.id, call.message.message_id)
                except Exception as e:
                    logger.warning(f"[PAYMENT] Не удалось удалить сообщение: {e}")
                bot_instance.answer_callback_query(call.id)
                return
        
            if action.startswith("expand:"):
                # Расширение подписки - создаем платеж на разницу
                parts = action.split(":")
                new_size = int(parts[1])  # 5 или 10
                subscription_id = int(parts[2])
            
                from moviebot.database.db_operations import (
                    get_subscription_by_id, get_active_group_users
                )
            
                # Получаем информацию о подписке
                sub = get_subscription_by_id(subscription_id)
                if not sub or sub.get('user_id') != user_id:
                    bot_instance.answer_callback_query(call.id, "Подписка не найдена", show_alert=True)
                    return
                
                # Если в личке, используем chat_id из подписки
                if is_private:
                    chat_id = sub.get('chat_id')
            
                current_size = sub.get('group_size') or 2
                plan_type = sub.get('plan_type')
                period_type = sub.get('period_type')
                group_chat_id = sub.get('chat_id')
            
                # Вычисляем разницу в цене
                current_price_base = SUBSCRIPTION_PRICES['group'][str(current_size)][plan_type].get(period_type, 0)
                new_price_base = SUBSCRIPTION_PRICES['group'][str(new_size)][plan_type].get(period_type, 0)
                diff = new_price_base - current_price_base
            
                # Применяем скидку, если есть личная подписка
                from moviebot.database.db_operations import get_user_personal_subscriptions
                personal_subs = get_user_personal_subscriptions(user_id)
                if personal_subs:
                    if new_size == 5:
                        diff = int(diff * 0.5)  # Скидка 50%
                    elif new_size == 10:
                        diff = int(new_price_base * 0.5) - current_price_base
                
                if diff <= 0:
                    bot_instance.answer_callback_query(call.id, "Ошибка: неверная сумма доплаты", show_alert=True)
                    return
                
                # Сохраняем состояние для создания платежа на расширение
                user_payment_state[user_id] = {
                    'step': 'pay',
                    'subscription_type': 'group',
                    'plan_type': plan_type,
                    'period_type': period_type,
                    'price': diff,
                    'group_size': new_size,
                    'chat_id': group_chat_id,
                    'group_username': sub.get('group_username'),
                    'telegram_username': call.from_user.username,
                    'is_expansion': True,
                    'expansion_subscription_id': subscription_id,
                    'expansion_current_size': current_size,
                    'expansion_new_size': new_size
                }
                
                # Показываем информацию о расширении и кнопку оплаты
                text = f"📈 <b>Расширение подписки</b>\n\n"
                text += f"Текущий размер: <b>{current_size} участников</b>\n"
                text += f"Новый размер: <b>{new_size} участников</b>\n"
                text += f"💰 Доплата: <b>{diff}₽</b>\n\n"
                text += "Нажмите кнопку ниже, чтобы оплатить расширение:"
                
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton(f"💳 Оплатить {diff}₽", callback_data=f"payment:pay:group:{new_size}:{plan_type}:{period_type}"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group:current"))
                
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action.startswith("add_member:"):
                # Добавление участника в подписку через кнопку
                parts = action.split(":")
                member_user_id = int(parts[1])
                subscription_id = int(parts[2])
            
                from moviebot.database.db_operations import get_subscription_members, add_subscription_member, get_active_group_users, get_subscription_by_id
            
                try:
                    # Проверяем, что подписка существует и принадлежит пользователю или группе
                    sub = get_subscription_by_id(subscription_id)
                    if not sub:
                        bot_instance.answer_callback_query(call.id, "Подписка не найдена", show_alert=True)
                        return
                
                    # Проверяем, что пользователь имеет право добавлять участников (владелец подписки)
                    if sub.get('user_id') != user_id:
                        bot_instance.answer_callback_query(call.id, "Только владелец подписки может добавлять участников", show_alert=True)
                        return
                
                    existing_members = get_subscription_members(subscription_id)
                
                    # Исключаем бота
                    if BOT_ID and BOT_ID in existing_members:
                        existing_members = {uid: uname for uid, uname in existing_members.items() if uid != BOT_ID}
                
                    if member_user_id in existing_members:
                        bot_instance.answer_callback_query(call.id, "Участник уже в подписке", show_alert=True)
                        return
                
                    # Проверяем лимит участников
                    group_size = sub.get('group_size')
                    if group_size and len(existing_members) >= int(group_size):
                        bot_instance.answer_callback_query(call.id, f"Достигнут лимит участников ({group_size})", show_alert=True)
                        return
                
                    # Добавляем участника
                    active_users = get_active_group_users(sub.get('chat_id', chat_id))
                    username = active_users.get(member_user_id, f"user_{member_user_id}")
                    add_subscription_member(subscription_id, member_user_id, username)
                    bot_instance.answer_callback_query(call.id, f"✅ Участник @{username} добавлен")
                
                    # Обновляем сообщение с информацией о подписке
                    # Можно отправить обновленное сообщение или просто подтвердить
                    logger.info(f"[PAYMENT] Участник {member_user_id} добавлен в подписку {subscription_id}")
                except Exception as e:
                    logger.error(f"[PAYMENT] Ошибка добавления участника: {e}", exc_info=True)
                    bot_instance.answer_callback_query(call.id, "Ошибка добавления участника", show_alert=True)
                return
        
            if action.startswith("toggle_member:"):
                # Переключение выбора участника при расширении
                parts = action.split(":")
                member_user_id = int(parts[1])
                subscription_id = int(parts[2])
            
                from moviebot.database.db_operations import get_subscription_members, add_subscription_member, get_active_group_users
            
                existing_members = get_subscription_members(subscription_id)
                # Исключаем бота
                if BOT_ID and BOT_ID in existing_members:
                    existing_members = {uid: uname for uid, uname in existing_members.items() if uid != BOT_ID}
            
                state = user_payment_state.get(user_id, {})
            
                if member_user_id in existing_members:
                    # Удаляем участника (нужно добавить функцию удаления)
                    # Пока просто пропускаем
                    bot_instance.answer_callback_query(call.id, "Участник уже в подписке")
                    return
                else:
                    # Добавляем участника
                    active_users = get_active_group_users(state.get('chat_id', chat_id))
                    username = active_users.get(member_user_id, f"user_{member_user_id}")
                    add_subscription_member(subscription_id, member_user_id, username)
                    bot_instance.answer_callback_query(call.id, "Участник добавлен")
            
                # Обновляем список
                return
        
            if action.startswith("toggle_member_sub:"):
                # Переключение выбора участника при создании подписки
                member_user_id = int(action.split(":")[1])
                state = user_payment_state.get(user_id, {})
            
                if 'selected_members' not in state:
                    state['selected_members'] = set()
            
                if member_user_id in state['selected_members']:
                    state['selected_members'].remove(member_user_id)
                    bot_instance.answer_callback_query(call.id, "Участник удален из выбора")
                else:
                    group_size = int(state.get('group_size', 2))
                    if len(state['selected_members']) >= group_size:
                        bot_instance.answer_callback_query(call.id, f"Можно выбрать только {group_size} участников", show_alert=True)
                        return
                    state['selected_members'].add(member_user_id)
                    bot_instance.answer_callback_query(call.id, "Участник добавлен в выбор")
            
                # Обновляем сообщение
                from moviebot.database.db_operations import get_active_group_users
                active_users = get_active_group_users(state.get('group_chat_id', chat_id))
                group_size = int(state.get('group_size', 2))
                selected_members = state.get('selected_members', set())
            
                text = f"⚠️ <b>Внимание!</b>\n\n"
                text += f"В группе <b>{len(active_users)}</b> активных участников, а вы выбираете подписку на <b>{group_size}</b>.\n\n"
                text += "Выберите участников для подписки:"
            
                markup = InlineKeyboardMarkup(row_width=1)
                for user_id_member, username_member in list(active_users.items())[:20]:
                    is_selected = user_id_member in selected_members
                    prefix = "✅" if is_selected else "⬜"
                    markup.add(InlineKeyboardButton(
                        f"{prefix} @{username_member}",
                        callback_data=f"payment:toggle_member_sub:{user_id_member}"
                    ))
            
                if len(selected_members) >= group_size:
                    markup.add(InlineKeyboardButton("✅ Подтвердить выбор", callback_data="payment:confirm_member_selection"))
                else:
                    markup.add(InlineKeyboardButton(f"✅ Подтвердить выбор ({len(selected_members)}/{group_size})", callback_data="payment:confirm_member_selection"))
            
                markup.add(InlineKeyboardButton("◀️ Отмена", callback_data="payment:back"))
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action.startswith("select_members:"):
                # Выбор участников для существующей подписки (после оплаты)
                subscription_id = int(action.split(":")[1])
            
                from moviebot.database.db_operations import get_active_group_users, get_subscription_members, get_subscription_by_id
            
                # Получаем информацию о подписке
                sub = get_subscription_by_id(subscription_id)
                if not sub:
                    bot_instance.answer_callback_query(call.id, "Подписка не найдена", show_alert=True)
                    return
            
                group_chat_id = sub.get('chat_id')
                group_size = sub.get('group_size')
            
                if not group_chat_id:
                    bot_instance.answer_callback_query(call.id, "Не удалось определить группу", show_alert=True)
                    return
            
                # Получаем активных пользователей и текущих участников подписки
                active_users = get_active_group_users(group_chat_id, bot_id=BOT_ID)
                existing_members_dict = get_subscription_members(subscription_id)
                # Исключаем бота из списка участников
                if BOT_ID and BOT_ID in existing_members_dict:
                    existing_members_dict = {uid: uname for uid, uname in existing_members_dict.items() if uid != BOT_ID}
                # get_subscription_members возвращает dict {user_id: username}
                existing_member_ids = set(existing_members_dict.keys()) if existing_members_dict else set()
            
                active_count = len(active_users) if active_users else 0
            
                if not active_users or active_count == 0:
                    bot_instance.answer_callback_query(call.id, "В группе нет активных участников", show_alert=True)
                    return
            
                # Сохраняем состояние для выбора участников
                user_payment_state[user_id] = {
                    'step': 'select_members_existing',
                    'subscription_id': subscription_id,
                    'chat_id': group_chat_id,
                    'group_size': group_size,
                    'selected_members': existing_member_ids.copy()
                }
            
                text = f"👥 <b>Выбор участников для подписки</b>\n\n"
                text += f"Подписка рассчитана на <b>{group_size}</b> участников\n"
                text += f"В группе <b>{active_count}</b> активных участников\n"
                text += f"Уже выбрано: <b>{len(existing_member_ids)}</b>\n\n"
                text += "Выберите участников:"
            
                markup = InlineKeyboardMarkup(row_width=1)
                for user_id_member, username_member in list(active_users.items())[:20]:
                    is_selected = user_id_member in existing_member_ids
                    prefix = "✅" if is_selected else "⬜"
                    markup.add(InlineKeyboardButton(
                        f"{prefix} @{username_member}",
                        callback_data=f"payment:toggle_member_existing:{user_id_member}:{subscription_id}"
                    ))
            
                remaining_slots = (group_size or active_count) - len(existing_member_ids)
                if remaining_slots > 0:
                    markup.add(InlineKeyboardButton(f"✅ Сохранить выбор ({len(existing_member_ids)}/{group_size or active_count})", callback_data=f"payment:confirm_members_existing:{subscription_id}"))
                else:
                    markup.add(InlineKeyboardButton("✅ Сохранить выбор", callback_data=f"payment:confirm_members_existing:{subscription_id}"))
            
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group"))
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action.startswith("toggle_member_existing:"):
                # Переключение выбора участника для существующей подписки
                parts = action.split(":")
                member_user_id = int(parts[1])
                subscription_id = int(parts[2])
            
                from moviebot.database.db_operations import get_active_group_users, get_subscription_members, add_subscription_member, remove_subscription_member, get_subscription_by_id
            
                state = user_payment_state.get(user_id, {})
                if state.get('subscription_id') != subscription_id:
                    bot_instance.answer_callback_query(call.id, "Ошибка состояния", show_alert=True)
                    return
            
                sub = get_subscription_by_id(subscription_id)
                if not sub:
                    bot_instance.answer_callback_query(call.id, "Подписка не найдена", show_alert=True)
                    return
            
                group_chat_id = sub.get('chat_id')
                group_size = sub.get('group_size')
            
                active_users = get_active_group_users(group_chat_id, bot_id=BOT_ID)
                existing_members_dict = get_subscription_members(subscription_id)
                # Исключаем бота из списка участников
                if BOT_ID and BOT_ID in existing_members_dict:
                    existing_members_dict = {uid: uname for uid, uname in existing_members_dict.items() if uid != BOT_ID}
                # get_subscription_members возвращает dict {user_id: username}
                existing_member_ids = set(existing_members_dict.keys()) if existing_members_dict else set()
            
                selected_members = state.get('selected_members', existing_member_ids.copy())
            
                if member_user_id in selected_members:
                    # Удаляем участника
                    selected_members.remove(member_user_id)
                    if member_user_id in existing_member_ids:
                        remove_subscription_member(subscription_id, member_user_id)
                    bot_instance.answer_callback_query(call.id, "Участник удален")
                else:
                    # Проверяем лимит
                    if group_size and len(selected_members) >= group_size:
                        bot_instance.answer_callback_query(call.id, f"Можно выбрать только {group_size} участников", show_alert=True)
                        return
                    # Добавляем участника
                    selected_members.add(member_user_id)
                    username = active_users.get(member_user_id, f"user_{member_user_id}")
                    if member_user_id not in existing_member_ids:
                        add_subscription_member(subscription_id, member_user_id, username)
                    bot_instance.answer_callback_query(call.id, "Участник добавлен")
            
                state['selected_members'] = selected_members
            
                # Обновляем сообщение
                text = f"👥 <b>Выбор участников для подписки</b>\n\n"
                text += f"Подписка рассчитана на <b>{group_size}</b> участников\n"
                text += f"В группе <b>{len(active_users)}</b> активных участников\n"
                text += f"Выбрано: <b>{len(selected_members)}</b>\n\n"
                text += "Выберите участников:"
            
                markup = InlineKeyboardMarkup(row_width=1)
                for user_id_member, username_member in list(active_users.items())[:20]:
                    is_selected = user_id_member in selected_members
                    prefix = "✅" if is_selected else "⬜"
                    markup.add(InlineKeyboardButton(
                        f"{prefix} @{username_member}",
                        callback_data=f"payment:toggle_member_existing:{user_id_member}:{subscription_id}"
                    ))
            
                remaining_slots = (group_size or len(active_users)) - len(selected_members)
                if remaining_slots > 0:
                    markup.add(InlineKeyboardButton(f"✅ Сохранить выбор ({len(selected_members)}/{group_size or len(active_users)})", callback_data=f"payment:confirm_members_existing:{subscription_id}"))
                else:
                    markup.add(InlineKeyboardButton("✅ Сохранить выбор", callback_data=f"payment:confirm_members_existing:{subscription_id}"))
            
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group"))
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action.startswith("confirm_members_existing:"):
                # Подтверждение выбора участников для существующей подписки
                subscription_id = int(action.split(":")[1])
            
                from moviebot.database.db_operations import get_subscription_members, get_subscription_by_id
            
                sub = get_subscription_by_id(subscription_id)
                if not sub:
                    bot_instance.answer_callback_query(call.id, "Подписка не найдена", show_alert=True)
                    return
            
                members = get_subscription_members(subscription_id)
                # Исключаем бота из списка участников
                if BOT_ID and BOT_ID in members:
                    members = {uid: uname for uid, uname in members.items() if uid != BOT_ID}
                members_count = len(members) if members else 0
            
                text = f"✅ <b>Участники сохранены</b>\n\n"
                text += f"Участников в подписке: <b>{members_count}</b>\n"
                if sub.get('group_size'):
                    text += f"Лимит: <b>{sub.get('group_size')}</b> участников\n"
            
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("👥 Список участников", callback_data=f"payment:group_members:{subscription_id}"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group"))
            
                if user_id in user_payment_state:
                    del user_payment_state[user_id]
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action == "confirm_member_selection":
                # Подтверждение выбора участников при создании подписки
                state = user_payment_state.get(user_id, {})
                selected_members = state.get('selected_members', set())
                group_size = int(state.get('group_size', 2))
            
                if len(selected_members) < group_size:
                    bot_instance.answer_callback_query(call.id, f"Нужно выбрать {group_size} участников", show_alert=True)
                    return
            
                # Переходим к подтверждению подписки
                state['step'] = 'confirm_group'
            
                # Показываем подтверждение
                username = state.get('group_username', '')
                plan_type = state.get('plan_type')
                period_type = state.get('period_type')
                price = state.get('price')
            
                text = f"👥 <b>Подтверждение групповой подписки</b>\n\n"
                text += f"Группа: <b>@{username}</b>\n"
                text += f"Количество участников: <b>{group_size}</b>\n"
                text += f"Выбрано участников: <b>{len(selected_members)}</b>\n\n"
                text += f"💰 <b>Стоимость:</b> {price}₽"
                if period_type != 'month':
                    period_names = {'3months': '3 месяца', 'year': 'год', 'lifetime': 'навсегда'}
                    period_name = period_names.get(period_type, period_type)
                    text += f" за {period_name}"
                text += "\n\nДля завершения оплаты свяжитесь с администратором."
            
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:back"))
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action.startswith("confirm_expansion:"):
                # Подтверждение расширения с выбранными участниками
                parts = action.split(":")
                subscription_id = int(parts[1])
                new_size = int(parts[2])
            
                from moviebot.database.db_operations import (
                    update_subscription_group_size, get_subscription_members
                )
            
                state = user_payment_state.get(user_id, {})
                diff_price = state.get('diff_price', 0)
            
                # Обновляем размер подписки
                update_subscription_group_size(subscription_id, new_size, diff_price)
            
                members = get_subscription_members(subscription_id)
                # Исключаем бота из списка участников
                if BOT_ID and BOT_ID in members:
                    members = {uid: uname for uid, uname in members.items() if uid != BOT_ID}
                members_count = len(members) if members else 0
            
                text = f"✅ <b>Подписка расширена</b>\n\n"
                text += f"Новый размер: <b>{new_size}</b> участников\n"
                text += f"✅ Участников в подписке: <b>{members_count}</b>\n\n"
                text += f"💰 Доплата: <b>{diff_price}₽</b>\n\n"
            
                # Если участников меньше, чем новый размер, предлагаем добавить
                if members_count < new_size:
                    text += f"💡 <b>Можно добавить еще {new_size - members_count} участников в подписку.</b>\n\n"
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("👥 Добавить участников", callback_data=f"payment:select_members:{subscription_id}"))
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group:current"))
                else:
                    text += "Для завершения оплаты свяжитесь с администратором."
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group:current"))
            
                if user_id in user_payment_state:
                    del user_payment_state[user_id]
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action == "active:group:other":
                # Проверка подписки другой группы - показываем список групп
                try:
                    bot_instance.answer_callback_query(call.id)
                except:
                    pass
            
                from moviebot.database.db_operations import get_user_groups
                user_groups = get_user_groups(user_id, bot)
            
                if not user_groups:
                    text = "👥 <b>Проверка групповой подписки</b>\n\n"
                    text += "❌ Не найдено групп, где вы и бот состоите вместе.\n\n"
                    text += "Добавьте бота в группу и отправьте любое сообщение, чтобы группа появилась в списке."
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group"))
                    try:
                        bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    except Exception as e:
                        if "message is not modified" not in str(e):
                            logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                    return
            
                # Дедупликация по chat_id
                seen_chat_ids = set()
                unique_groups = []
                for group in user_groups:
                    chat_id = group.get('chat_id')
                    if chat_id and chat_id not in seen_chat_ids:
                        seen_chat_ids.add(chat_id)
                        unique_groups.append(group)
                user_groups = unique_groups
            
                # Показываем список групп для выбора
                text = "👥 <b>Проверка групповой подписки</b>\n\n"
                text += "Выберите группу из списка:"
            
                markup = InlineKeyboardMarkup(row_width=1)
                for group in user_groups[:10]:  # Ограничиваем до 10 групп
                    group_title = group.get('title', f"Группа {group.get('chat_id')}")
                    group_username = group.get('username')
                    if group_username:
                        button_text = f"📍 {group_title} (@{group_username})"
                    else:
                        button_text = f"📍 {group_title}"
                    # Ограничиваем длину текста кнопки
                    if len(button_text) > 50:
                        button_text = button_text[:47] + "..."
                    markup.add(InlineKeyboardButton(
                        button_text,
                        callback_data=f"payment:check_group:{group.get('chat_id')}"
                    ))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group"))
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action.startswith("check_group:"):
                # Проверка подписки выбранной группы
                group_chat_id = int(action.split(":")[1])
            
                try:
                    bot_instance.answer_callback_query(call.id)
                except:
                    pass
            
                from moviebot.database.db_operations import get_subscription_members, get_active_group_users
            
                # Получаем информацию о группе
                try:
                    chat = bot_instance.get_chat(group_chat_id)
                    group_username = chat.username
                    group_title = chat.title
                except Exception as e:
                    logger.error(f"[PAYMENT] Ошибка получения информации о группе {group_chat_id}: {e}")
                    bot_instance.answer_callback_query(call.id, "Ошибка получения информации о группе", show_alert=True)
                    return
            
                sub = get_active_subscription(group_chat_id, user_id, 'group')
            
                # Если подписки нет, но бот присутствует в группе, создаем виртуальную подписку
                if not sub:
                    # Проверяем наличие активности в группе
                    active_users = get_active_group_users(group_chat_id, bot_id=BOT_ID)
                    if active_users:
                        # Создаем виртуальную подписку
                        now = datetime.now(pytz.UTC)
                        sub = {
                            'id': -1,
                            'chat_id': group_chat_id,
                            'user_id': user_id,
                            'subscription_type': 'group',
                            'plan_type': 'all',
                            'period_type': 'lifetime',
                            'price': 0,
                            'activated_at': now,
                            'next_payment_date': None,
                            'expires_at': None,
                            'is_active': True,
                            'cancelled_at': None,
                            'telegram_username': None,
                            'group_username': group_username,
                            'group_size': None,
                            'created_at': now
                        }
            
                if sub:
                    expires_at = sub.get('expires_at')
                    next_payment = sub.get('next_payment_date')
                    price = sub.get('price', 0)
                    activated = sub.get('activated_at')
                    group_size = sub.get('group_size')
                    subscription_id = sub.get('id')
                    plan_type = sub.get('plan_type', 'all')
                    period_type = sub.get('period_type', 'lifetime')
                
                    text = f"👥 <b>Групповая подписка</b>\n\n"
                    if plan_type == 'all':
                        text += f"📦 <b>Пакетная подписка - Все режимы</b>\n\n"
                    text += f"Группа: <b>{group_title}</b>\n"
                    if group_username:
                        text += f"@{group_username}\n"
                    text += f"\n💰 Сумма платежа: <b>{price}₽</b>\n"
                    if group_size:
                        text += f"👥 Количество участников: <b>{group_size}</b>\n"
                        if subscription_id and subscription_id > 0:
                            try:
                                members = get_subscription_members(subscription_id)
                                # Исключаем бота из списка участников
                                if BOT_ID and BOT_ID in members:
                                    members = {uid: uname for uid, uname in members.items() if uid != BOT_ID}
                                text += f"✅ Участников в подписке: <b>{len(members)}</b>\n"
                            except Exception as members_error:
                                logger.error(f"[PAYMENT] Ошибка получения участников подписки: {members_error}")
                                text += f"✅ Участников в подписке: <b>?</b>\n"
                    if activated:
                        text += f"📅 Дата активации: <b>{activated.strftime('%d.%m.%Y') if isinstance(activated, datetime) else activated}</b>\n"
                    if next_payment:
                        text += f"📅 Следующее списание: <b>{next_payment.strftime('%d.%m.%Y') if isinstance(next_payment, datetime) else next_payment}</b>\n"
                    if expires_at:
                        text += f"⏰ Действует до: <b>{expires_at.strftime('%d.%m.%Y') if isinstance(expires_at, datetime) else expires_at}</b>\n"
                    else:
                        text += f"⏰ Действует: <b>Навсегда</b>\n"
                
                    markup = InlineKeyboardMarkup(row_width=1)
                    if subscription_id and subscription_id > 0:
                        markup.add(InlineKeyboardButton("👥 Список участников", callback_data=f"payment:group_members:{subscription_id}"))
                
                    # Кнопки расширения подписки (только для реальных подписок с ограничением по участникам)
                    if subscription_id and subscription_id > 0 and (group_size is None or group_size == 2):
                        plan_type_sub = sub.get('plan_type')
                        period_type_sub = sub.get('period_type')
                        current_price = SUBSCRIPTION_PRICES['group']['2'][plan_type_sub].get(period_type_sub, 0)
                        price_5 = SUBSCRIPTION_PRICES['group']['5'][plan_type_sub].get(period_type_sub, 0)
                        price_10 = SUBSCRIPTION_PRICES['group']['10'][plan_type_sub].get(period_type_sub, 0)
                        diff_5 = price_5 - current_price
                        diff_10 = price_10 - current_price
                    
                        from moviebot.database.db_operations import get_user_personal_subscriptions
                        personal_subs = get_user_personal_subscriptions(user_id)
                        if personal_subs:
                            diff_5 = int(diff_5 * 0.5)
                            diff_10 = int(price_10 * 0.5) - current_price
                    
                        markup.add(InlineKeyboardButton(f"📈 Расширить до 5 (+{diff_5}₽)", callback_data=f"payment:expand:5:{subscription_id}"))
                        markup.add(InlineKeyboardButton(f"📈 Расширить до 10 (+{diff_10}₽)", callback_data=f"payment:expand:10:{subscription_id}"))
                    elif subscription_id and subscription_id > 0 and group_size == 5:
                        plan_type_sub = sub.get('plan_type')
                        period_type_sub = sub.get('period_type')
                        current_price = SUBSCRIPTION_PRICES['group']['5'][plan_type_sub].get(period_type_sub, 0)
                        price_10 = SUBSCRIPTION_PRICES['group']['10'][plan_type_sub].get(period_type_sub, 0)
                        diff_10 = price_10 - current_price
                    
                        from moviebot.database.db_operations import get_user_personal_subscriptions
                        personal_subs = get_user_personal_subscriptions(user_id)
                        if personal_subs:
                            diff_10 = int(price_10 * 0.5) - current_price
                    
                        markup.add(InlineKeyboardButton(f"📈 Расширить до 10 (+{diff_10}₽)", callback_data=f"payment:expand:10:{subscription_id}"))
                
                    # Показываем кнопки "Изменить" и "Отменить" для всех активных подписок
                    if subscription_id and subscription_id > 0:
                        markup.add(InlineKeyboardButton("✏️ Изменить подписку", callback_data=f"payment:modify:{subscription_id}"))
                        markup.add(InlineKeyboardButton("❌ Отменить", callback_data=f"payment:cancel:{subscription_id}"))
                    elif subscription_id == 0 or subscription_id is None:
                        # Для виртуальных подписок или подписок без id предлагаем тарифы
                        markup.add(InlineKeyboardButton("✏️ Изменить подписку", callback_data="payment:tariffs:group"))
                        markup.add(InlineKeyboardButton("❌ Отменить", callback_data="payment:cancel:group"))
                
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active"))
                else:
                    text = f"👥 <b>Групповая подписка</b>\n\n"
                    text += f"Группа: <b>{group_title}</b>\n"
                    if group_username:
                        text += f"@{group_username}\n"
                    text += "\n❌ Активная подписка отсутствует, выберите тариф для подключения"
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("💰 Тарифы", callback_data="payment:tariffs:group"))
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active"))
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action == "tariffs":
                # Показываем выбор типа подписки (личная/групповая)
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("👤 Личные", callback_data="payment:tariffs:personal"))
                markup.add(InlineKeyboardButton("👥 Групповые", callback_data="payment:tariffs:group"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:back"))
            
                try:
                    bot_instance.edit_message_text(
                        "💰 <b>Тарифы</b>\n\nВыберите тип подписки:",
                        call.message.chat.id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            # Общая проверка для групповых подписок
            if action == "active:group":
                if is_private:
                    from moviebot.database.db_operations import get_user_groups
                    
                    user_groups = get_user_groups(user_id, bot)
                    
                    # Самая надёжная дедупликация
                    unique_groups = []
                    seen_chat_ids = set()
                    
                    for group in user_groups:
                        chat_id = group.get('chat_id')
                        # Приводим к одному типу и убираем возможные None
                        if chat_id is None:
                            continue
                            
                        # Важно! Приводим к int (telegram chat_id всегда целые числа)
                        try:
                            chat_id = int(chat_id)
                        except (ValueError, TypeError):
                            continue
                            
                        if chat_id not in seen_chat_ids:
                            seen_chat_ids.add(chat_id)
                            unique_groups.append(group)
                    
                    # ────────────── дальше твой текущий код без изменений ──────────────
                    if not unique_groups:
                        text = "👥 <b>Групповая подписка</b>\n\n"
                        text += "❌ Нет групп, где вы и бот состоите вместе.\n\n"
                        text += "Добавьте бота в группу и напишите любое сообщение."
                        
                        markup = InlineKeyboardMarkup()
                        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active"))
                    else:
                        text = "👥 <b>Групповая подписка</b>\n\n"
                        text += "Выберите группу для проверки:\n\n"
                        
                        markup = InlineKeyboardMarkup(row_width=1)
                        for group in unique_groups[:10]:  # лимит оставляем
                            title = group.get('title', f"Группа {group.get('chat_id')}")
                            username = group.get('username')
                            button_text = f"📍 {title}"
                            if username:
                                button_text += f" (@{username})"
                            if len(button_text) > 60:
                                button_text = button_text[:57] + "..."
                            markup.add(InlineKeyboardButton(
                                button_text,
                                callback_data=f"payment:check_group:{group.get('chat_id')}"
                            ))
                        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active"))
                    
                    bot.edit_message_text(
                        text,
                        call.message.chat.id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                    return
                
                else:
                    # В группе — показываем только текущую группу
                    try:
                        from moviebot.database.db_operations import get_subscription_members
                        
                        chat = bot.get_chat(chat_id)
                        group_title = chat.title or "Без названия"
                        group_username = chat.username
                        
                        sub = get_active_subscription(chat_id, user_id, 'group')
                        
                        text = "👥 <b>Групповая подписка</b>\n\n"
                        text += f"Группа: <b>{group_title}</b>\n"
                        if group_username:
                            text += f"@{group_username}\n"
                        
                        markup = InlineKeyboardMarkup(row_width=1)
                        
                        if sub:
                            # Есть активная подписка
                            expires_at = sub.get('expires_at')
                            next_payment = sub.get('next_payment_date')
                            price = sub.get('price', 0)
                            group_size = sub.get('group_size')
                            subscription_id = sub.get('id')
                            plan_type = sub.get('plan_type', 'all')
                            
                            text += f"\n💰 Сумма: <b>{price}₽</b>\n"
                            if group_size:
                                text += f"👥 Участников: <b>{group_size}</b>\n"
                                members = get_subscription_members(subscription_id)
                                if members and BOT_ID in members:
                                    members = {k: v for k, v in members.items() if k != BOT_ID}
                                text += f"✅ В подписке: <b>{len(members)}</b>\n"
                            if expires_at:
                                text += f"⏰ До: <b>{expires_at.strftime('%d.%m.%Y') if isinstance(expires_at, datetime) else expires_at}</b>\n"
                            else:
                                text += "⏰ <b>Навсегда</b>\n"
                            
                            if subscription_id:
                                markup.add(InlineKeyboardButton("👥 Участники", callback_data=f"payment:group_members:{subscription_id}"))
                                markup.add(InlineKeyboardButton("✏️ Изменить", callback_data=f"payment:modify:{subscription_id}"))
                                markup.add(InlineKeyboardButton("❌ Отменить", callback_data=f"payment:cancel:{subscription_id}"))
                        else:
                            # Нет подписки
                            text += "\n❌ Активная подписка отсутствует"
                            markup.add(InlineKeyboardButton("💰 Подключить", callback_data="payment:tariffs:group"))
                        
                        # Кнопка "Другие группы" только в группе
                        markup.add(InlineKeyboardButton("📍 Другие группы", callback_data="payment:active:group:other"))
                        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active"))
                        
                        bot.edit_message_text(
                            text,
                            call.message.chat.id,
                            call.message.message_id,
                            reply_markup=markup,
                            parse_mode='HTML'
                        )
                        return
                        
                    except Exception as e:
                        logger.error(f"Ошибка получения информации о группе: {e}")
                        bot.answer_callback_query(call.id, "Ошибка", show_alert=True)
                        return
        
            if action.startswith("tariffs:personal"):
                # Сохраняем информацию о том, откуда пришли (из действующей подписки или из главного меню)
                # Проверяем, есть ли в callback_data информация о том, что это из действующей подписки
                if "modify" in str(call.data) or "active" in str(call.data):
                    user_payment_state[user_id] = user_payment_state.get(user_id, {})
                    user_payment_state[user_id]['from_active'] = True
            
                # Тарифы для личных подписок
                text = "👤 <b>Личные тарифы</b>\n\n"
            
                # Описание бесплатных функций
                text += "🆓 <b>Бесплатные функции:</b>\n"
                text += "• Добавление фильмов в базу\n"
                text += "• Отметка просмотренных фильмов\n"
                text += "• Планирование просмотра\n"
                text += "• Базовый рандомный выбор фильма\n"
                text += "• Статистика\n\n"
            
                # Описание платных функций
                text += "💎 <b>Платные функции:</b>\n\n"
                text += "🔔 <b>Уведомления о сериалах:</b> 100₽/мес\n"
                text += "   • Уведомления о новых сериях\n"
                text += "   • Настройка времени уведомлений\n"
                text += "   • Отслеживание прогресса просмотра сезонов\n\n"
                text += "🎯 <b>Персональные рекомендации:</b> 100₽/мес\n"
                text += "Вы сможете не просто найти фильм из ранее отложенных к просмотру, но и получить рекомендацию, основываясь на ваших личных или групповых оценках. Вы сможете найти новый фильм, который вам точно подойдет!\n"
                text += "   • Режим \"По оценкам в базе\" — рекомендации по оценкам фильмов, добавленных в базу чата или группы\n"
                text += "   • Режим \"Рандом по Кинопоиску\" — случайный фильм из Кинопоиска по фильтрам\n"
                text += "   • Режим рандомайзера \"По моим оценкам\" — рекомендации по оценкам из Кинопоиска\n"
                text += "   • Импорт базы из Кинопоиска\n\n"
                text += "🎫 <b>Билеты в кино:</b> 150₽/мес\n"
                text += "Вы сможете добавлять билеты на сеансы и любые другие мероприятия в бот, и они всегда будут в доступе по одной кнопке. В день мероприятия вам придет уведомление, а за непосредственно перед мероприятием бот пришлет билеты, чтобы не пришлось их искать на входе. Мы не храним и не обрабатываем файлы.\n"
                text += "   • Добавление билетов на сеансы и мероприятия\n"
                text += "   • Настраиваемые уведомления с билетами перед мероприятием\n\n"
                text += "📦 <b>Все режимы:</b>\n"
                text += "• 249₽/мес\n"
                text += "• 599₽ за 3 месяца\n"
                text += "• 1799₽ за год\n"
                text += "• 2299₽ навсегда\n\n"
            
                # Проверяем существующие подписки
                from moviebot.database.db_operations import get_user_personal_subscriptions
                existing_subs = get_user_personal_subscriptions(user_id)
            
                # Фильтруем только активные подписки и убираем дубликаты по plan_type
                active_subs = []
                seen_plan_types = set()
                now = datetime.now(pytz.UTC)  # Используем UTC для сравнения с датами из БД
            
                for sub in existing_subs:
                    expires_at = sub.get('expires_at')
                    plan_type = sub.get('plan_type')
                
                    # Проверяем, что подписка активна
                    is_active = False
                    if expires_at:
                        if isinstance(expires_at, datetime):
                            # Приводим expires_at к aware datetime, если он naive
                            if expires_at.tzinfo is None:
                                expires_at = pytz.UTC.localize(expires_at)
                            # Приводим к UTC для корректного сравнения
                            if expires_at.tzinfo != pytz.UTC:
                                expires_at = expires_at.astimezone(pytz.UTC)
                            is_active = expires_at > now
                        else:
                            # Если expires_at - это строка или другой тип, пытаемся преобразовать
                            try:
                                if isinstance(expires_at, str):
                                    expires_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                                    # Убеждаемся, что datetime aware
                                    if expires_dt.tzinfo is None:
                                        expires_dt = pytz.UTC.localize(expires_dt)
                                    # Приводим к UTC для корректного сравнения
                                    if expires_dt.tzinfo != pytz.UTC:
                                        expires_dt = expires_dt.astimezone(pytz.UTC)
                                    is_active = expires_dt > now
                                else:
                                    is_active = True  # Если не можем проверить, считаем активной
                            except:
                                is_active = True
                    else:
                        # Если нет expires_at, считаем подписку активной (lifetime)
                        is_active = True
                
                    # Добавляем только активные и уникальные по plan_type
                    if is_active and plan_type and plan_type not in seen_plan_types:
                        active_subs.append(sub)
                        seen_plan_types.add(plan_type)
            
                existing_plan_types = [sub.get('plan_type') for sub in active_subs if sub.get('plan_type')]
                has_all = 'all' in existing_plan_types
            
                if active_subs and not has_all:
                    # Есть подписки, но нет пакетной
                    text += "⚠️ <b>У вас уже есть активные подписки:</b>\n"
                    for sub in active_subs:
                        plan_type = sub.get('plan_type')
                        plan_names = {
                            'notifications': 'Уведомления о сериалах',
                            'recommendations': 'Рекомендации',
                            'tickets': 'Билеты'
                        }
                        plan_name = plan_names.get(plan_type, plan_type)
                        text += f"• {plan_name}\n"
                    text += "\n"
            
                text += "Выберите тариф:"
            
                markup = InlineKeyboardMarkup(row_width=1)
            
                # Если есть пакетная подписка, не показываем другие тарифы
                if has_all:
                    text += "\n\n⚠️ <b>У вас уже есть подписка \"Все режимы\".</b>\n"
                    text += "Вы не можете добавить дополнительные подписки к пакетной."
                else:
                    # Показываем только те тарифы, которых у пользователя НЕТ
                    if 'notifications' not in existing_plan_types:
                        markup.add(InlineKeyboardButton("🔔 Уведомления (100₽/мес)", callback_data="payment:subscribe:personal:notifications:month"))
                    if 'recommendations' not in existing_plan_types:
                        markup.add(InlineKeyboardButton("🎯 Рекомендации (100₽/мес)", callback_data="payment:subscribe:personal:recommendations:month"))
                    if 'tickets' not in existing_plan_types:
                        markup.add(InlineKeyboardButton("🎫 Билеты (150₽/мес)", callback_data="payment:subscribe:personal:tickets:month"))
                    # "Все режимы" всегда показываем, так как это замена текущих подписок
                    markup.add(InlineKeyboardButton("📦 Все режимы - месяц (249₽/мес)", callback_data="payment:subscribe:personal:all:month"))
                    markup.add(InlineKeyboardButton("📦 Все режимы - 3 месяца (599₽)", callback_data="payment:subscribe:personal:all:3months"))
                    markup.add(InlineKeyboardButton("📦 Все режимы - год (1799₽)", callback_data="payment:subscribe:personal:all:year"))
                    markup.add(InlineKeyboardButton("📦 Все режимы - навсегда (2299₽)", callback_data="payment:subscribe:personal:all:lifetime"))
                    
                    # Тестовый тариф только для владельца бота
                    from moviebot.bot.handlers.promo import get_bot_owner_id
                    owner_id = get_bot_owner_id()
                    if owner_id and user_id == owner_id:
                        markup.add(InlineKeyboardButton("🧪 Тестовый тариф (10₽, раз в 10 мин)", callback_data="payment:subscribe:personal:test:test"))
                # Проверяем, откуда пришли в тарифы (из действующей подписки или из главного меню)
                back_callback = "payment:active:personal" if action == "tariffs:personal" and user_payment_state.get(user_id, {}).get('from_active') else "payment:tariffs"
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data=back_callback))
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action.startswith("tariffs:group"):
                # Сохраняем информацию о том, откуда пришли (из действующей подписки или из главного меню)
                # Проверяем, есть ли в callback_data информация о том, что это из действующей подписки
                if "modify" in str(call.data) or "active" in str(call.data):
                    user_payment_state[user_id] = user_payment_state.get(user_id, {})
                    user_payment_state[user_id]['from_active'] = True
            
                # Тарифы для групповых подписок - сначала выбор количества участников
                text = "👥 <b>Групповые тарифы</b>\n\n"
            
                # Если команда вызвана в группе, добавляем предупреждение
                if not is_private and chat_id < 0:
                    text += "⚠️ <i>В группе доступны тарифы только для текущей группы.</i>\n\n"
                    text += "💬 <i>Если хотите посмотреть остальные группы, в которых вы состоите, напишите в личку боту.</i>\n\n"
            
                text += "Выберите количество участников в группе:\n\n"
                text += "💡 <i>Стоимость зависит от количества участников</i>"
            
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("👥 2 участника", callback_data="payment:group_size:2"))
                markup.add(InlineKeyboardButton("👥 5 участников", callback_data="payment:group_size:5"))
                markup.add(InlineKeyboardButton("👥 10 участников", callback_data="payment:group_size:10"))
                # Проверяем, откуда пришли в тарифы (из действующей подписки или из главного меню)
                back_callback = "payment:active:group:current" if user_payment_state.get(user_id, {}).get('from_active') else "payment:tariffs"
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data=back_callback))
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action.startswith("group_size:"):
                # Выбор тарифа для конкретного количества участников
                group_size = action.split(":")[1]  # 2, 5 или 10
                prices = SUBSCRIPTION_PRICES['group'][group_size]
            
                text = f"👥 <b>Групповые тарифы на {group_size} участников</b>\n\n"
            
                # Описание бесплатных функций
                text += "🆓 <b>Бесплатные функции:</b>\n"
                text += "• Добавление фильмов в базу\n"
                text += "• Отметка просмотренных фильмов\n"
                text += "• Планирование просмотра\n"
                text += "• Базовый рандомный выбор фильма\n"
                text += "• Статистика группы\n\n"
            
                # Описание платных функций
                text += "💎 <b>Платные функции:</b>\n\n"
                text += f"🔔 <b>Уведомления о сериалах:</b> {prices['notifications']['month']}₽/мес\n"
                text += "   • Уведомления о новых сериях\n"
                text += "   • Настройка времени уведомлений\n"
                text += "   • Отслеживание прогресса просмотра сезонов\n\n"
                text += f"🎯 <b>Персональные рекомендации:</b> {prices['recommendations']['month']}₽/мес\n"
                text += "Вы сможете не просто найти фильм из ранее отложенных к просмотру, но и получить рекомендацию, основываясь на ваших личных или групповых оценках. Вы сможете найти новый фильм, который вам точно подойдет!\n"
                text += "   • Режим \"По оценкам в базе\" — рекомендации по оценкам фильмов, добавленных в базу чата или группы\n"
                text += "   • Режим \"Рандом по Кинопоиску\" — случайный фильм из Кинопоиска по фильтрам\n"
                text += "   • Режим рандомайзера \"По моим оценкам\" — рекомендации по оценкам из Кинопоиска\n"
                text += "   • Импорт базы из Кинопоиска\n\n"
                text += f"🎫 <b>Билеты в кино:</b> {prices['tickets']['month']}₽/мес\n"
                text += "Вы сможете добавлять билеты на сеансы и любые другие мероприятия в бот, и они всегда будут в доступе по одной кнопке. В день мероприятия вам придет уведомление, а за непосредственно перед мероприятием бот пришлет билеты, чтобы не пришлось их искать на входе. Мы не храним и не обрабатываем файлы.\n"
                text += "   • Добавление билетов на сеансы и мероприятия\n"
                text += "   • Настраиваемые уведомления с билетами перед мероприятием\n\n"
                text += f"📦 <b>Все режимы:</b>\n"
                text += f"• {prices['all']['month']}₽/мес\n"
                text += f"• {prices['all']['3months']}₽ за 3 месяца\n"
                text += f"• {prices['all']['year']}₽ за год\n"
                text += f"• {prices['all']['lifetime']}₽ навсегда\n\n"
            
                # Информация о скидках
                from moviebot.database.db_operations import get_user_personal_subscriptions
                personal_subs = get_user_personal_subscriptions(user_id)
                if personal_subs:
                    if group_size == '2':
                        text += "💡 <i>У вас есть личная подписка - скидка 20% на группу из 2 человек</i>\n\n"
                    elif group_size in ['5', '10']:
                        text += "💡 <i>У вас есть личная подписка - скидка 50% на группу</i>\n\n"
            
                # Если команда вызвана в группе, используем текущую группу
                if not is_private and chat_id < 0:
                    # Это группа - используем текущую группу
                    try:
                        chat = bot_instance.get_chat(chat_id)
                        group_username = chat.username
                        group_title = chat.title
                    
                        # Сразу переходим к выбору тарифа для текущей группы
                        user_payment_state[user_id] = {
                            'subscription_type': 'group',
                            'group_size': group_size,
                            'group_chat_id': chat_id,
                            'group_username': group_username,
                            'group_title': group_title
                        }
                    
                        # Показываем тарифы для текущей группы
                        text = f"👥 <b>Групповые тарифы на {group_size} участников</b>\n\n"
                        text += f"Группа: <b>{group_title}</b>\n"
                        if group_username:
                            text += f"@{group_username}\n"
                        text += "\n"
                    
                        # Добавляем информацию о тарифах
                        text += "💎 <b>Платные функции:</b>\n\n"
                        text += f"🔔 Уведомления о сериалах: {prices['notifications']['month']}₽/мес\n"
                        text += f"🎯 Персональные рекомендации: {prices['recommendations']['month']}₽/мес\n"
                        text += f"🎫 Билеты в кино: {prices['tickets']['month']}₽/мес\n\n"
                        text += f"📦 Все режимы:\n"
                        text += f"• {prices['all']['month']}₽/мес\n"
                        text += f"• {prices['all']['3months']}₽ за 3 месяца\n"
                        text += f"• {prices['all']['year']}₽ за год\n"
                        text += f"• {prices['all']['lifetime']}₽ навсегда\n\n"
                    
                        # Информация о скидках
                        if personal_subs:
                            if group_size == '2':
                                text += "💡 <i>У вас есть личная подписка - скидка 20% на группу из 2 человек</i>\n\n"
                            elif group_size in ['5', '10']:
                                text += "💡 <i>У вас есть личная подписка - скидка 50% на группу</i>\n\n"
                    
                        # Проверяем активные групповые подписки для этой группы
                        from moviebot.database.db_operations import get_active_group_subscription_by_chat_id
                        group_sub = get_active_group_subscription_by_chat_id(chat_id)
                        existing_group_plan_types = []
                    
                        if group_sub:
                            group_plan_type = group_sub.get('plan_type')
                            if group_plan_type:
                                existing_group_plan_types.append(group_plan_type)
                    
                        text += "Выберите тариф:"
                    
                        markup = InlineKeyboardMarkup(row_width=1)
                        # Показываем только те тарифы, которых у группы НЕТ
                        if 'notifications' not in existing_group_plan_types:
                            markup.add(InlineKeyboardButton(f"🔔 Уведомления ({prices['notifications']['month']}₽/мес)", callback_data=f"payment:subscribe:group:{group_size}:notifications:month:{chat_id}"))
                        if 'recommendations' not in existing_group_plan_types:
                            markup.add(InlineKeyboardButton(f"🎯 Рекомендации ({prices['recommendations']['month']}₽/мес)", callback_data=f"payment:subscribe:group:{group_size}:recommendations:month:{chat_id}"))
                        if 'tickets' not in existing_group_plan_types:
                            markup.add(InlineKeyboardButton(f"🎫 Билеты ({prices['tickets']['month']}₽/мес)", callback_data=f"payment:subscribe:group:{group_size}:tickets:month:{chat_id}"))
                        # "Все режимы" всегда показываем, так как это замена текущих подписок
                        markup.add(InlineKeyboardButton(f"📦 Все режимы - месяц ({prices['all']['month']}₽/мес)", callback_data=f"payment:subscribe:group:{group_size}:all:month:{chat_id}"))
                        markup.add(InlineKeyboardButton(f"📦 Все режимы - 3 месяца ({prices['all']['3months']}₽/3 мес)", callback_data=f"payment:subscribe:group:{group_size}:all:3months:{chat_id}"))
                        markup.add(InlineKeyboardButton(f"📦 Все режимы - год ({prices['all']['year']}₽/год)", callback_data=f"payment:subscribe:group:{group_size}:all:year:{chat_id}"))
                        markup.add(InlineKeyboardButton(f"📦 Все режимы - навсегда ({prices['all']['lifetime']}₽)", callback_data=f"payment:subscribe:group:{group_size}:all:lifetime:{chat_id}"))
                        # Проверяем, откуда пришли в тарифы (из действующей подписки или из главного меню)
                        back_callback = "payment:active:group:current" if user_payment_state.get(user_id, {}).get('from_active') else "payment:tariffs:group"
                        markup.add(InlineKeyboardButton("◀️ Назад", callback_data=back_callback))
                    
                        try:
                            bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                        except Exception as e:
                            if "message is not modified" not in str(e):
                                logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                        return
                    except Exception as e:
                        logger.error(f"[PAYMENT] Ошибка получения информации о группе {chat_id}: {e}")
                        bot_instance.answer_callback_query(call.id, "Ошибка получения информации о группе", show_alert=True)
                        return
            
                # Если команда вызвана в личке, показываем список групп
                from moviebot.database.db_operations import get_user_groups
                try:
                    user_groups = get_user_groups(user_id, bot)
                except Exception as e:
                    logger.error(f"[PAYMENT] Ошибка получения групп пользователя: {e}", exc_info=True)
                    user_groups = []
            
                if not user_groups:
                    text = f"👥 <b>Групповые тарифы на {group_size} участников</b>\n\n"
                    text += "❌ Не найдено групп, где вы и бот состоите вместе.\n\n"
                    text += "Добавьте бота в группу и отправьте любое сообщение, чтобы группа появилась в списке."
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:tariffs:group"))
                    try:
                        bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    except Exception as e:
                        if "message is not modified" not in str(e):
                            logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                    return
            
                # Показываем выбор группы
                text = f"👥 <b>Выберите группу для подписки на {group_size} участников</b>\n\n"
                text += "Выберите группу из списка:"
            
                markup = InlineKeyboardMarkup(row_width=1)
                for group in user_groups[:10]:  # Ограничиваем до 10 групп
                    group_title = group.get('title', f"Группа {group.get('chat_id')}")
                    group_username = group.get('username')
                    if group_username:
                        button_text = f"📍 {group_title} (@{group_username})"
                    else:
                        button_text = f"📍 {group_title}"
                    # Ограничиваем длину текста кнопки
                    if len(button_text) > 50:
                        button_text = button_text[:47] + "..."
                    markup.add(InlineKeyboardButton(
                        button_text,
                        callback_data=f"payment:select_group:{group_size}:{group.get('chat_id')}"
                    ))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:tariffs:group"))
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action.startswith("select_group:"):
                # Выбор группы для групповой подписки
                parts = action.split(":")
                group_size = parts[1]
                group_chat_id = int(parts[2])
            
                # Получаем информацию о группе
                try:
                    chat = bot_instance.get_chat(group_chat_id)
                    group_username = chat.username
                    group_title = chat.title
                except Exception as e:
                    logger.error(f"[PAYMENT] Ошибка получения информации о группе {group_chat_id}: {e}")
                    bot_instance.answer_callback_query(call.id, "Ошибка получения информации о группе", show_alert=True)
                    return
            
                # Сохраняем выбранную группу в состоянии
                user_payment_state[user_id] = {
                    'subscription_type': 'group',
                    'group_size': group_size,
                    'group_chat_id': group_chat_id,
                    'group_username': group_username,
                    'group_title': group_title
                }
            
                # Показываем тарифы для выбранной группы
                prices = SUBSCRIPTION_PRICES['group'][group_size]
            
                text = f"👥 <b>Групповые тарифы на {group_size} участников</b>\n\n"
                text += f"Группа: <b>{group_title}</b>\n"
                if group_username:
                    text += f"@{group_username}\n"
                text += "\n"
            
                # Описание платных функций
                text += "💎 <b>Платные функции:</b>\n\n"
                text += f"🔔 <b>Уведомления о сериалах:</b> {prices['notifications']['month']}₽/мес\n"
                text += f"🎯 <b>Персональные рекомендации:</b> {prices['recommendations']['month']}₽/мес\n"
                text += f"🎫 <b>Билеты в кино:</b> {prices['tickets']['month']}₽/мес\n\n"
                text += f"📦 <b>Все режимы:</b>\n"
                text += f"• {prices['all']['month']}₽/мес\n"
                text += f"• {prices['all']['3months']}₽ за 3 месяца\n"
                text += f"• {prices['all']['year']}₽ за год\n"
                text += f"• {prices['all']['lifetime']}₽ навсегда\n\n"
            
                # Информация о скидках
                from moviebot.database.db_operations import get_user_personal_subscriptions
                personal_subs = get_user_personal_subscriptions(user_id)
                if personal_subs:
                    if group_size == '2':
                        text += "💡 <i>У вас есть личная подписка - скидка 20% на группу из 2 человек</i>\n\n"
                    elif group_size in ['5', '10']:
                        text += "💡 <i>У вас есть личная подписка - скидка 50% на группу</i>\n\n"
            
                text += "Выберите тариф:"
            
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton(f"🔔 Уведомления ({prices['notifications']['month']}₽/мес)", callback_data=f"payment:subscribe:group:{group_size}:notifications:month"))
                markup.add(InlineKeyboardButton(f"🎯 Рекомендации ({prices['recommendations']['month']}₽/мес)", callback_data=f"payment:subscribe:group:{group_size}:recommendations:month"))
                markup.add(InlineKeyboardButton(f"🎫 Билеты ({prices['tickets']['month']}₽/мес)", callback_data=f"payment:subscribe:group:{group_size}:tickets:month"))
                markup.add(InlineKeyboardButton(f"📦 Все режимы - месяц ({prices['all']['month']}₽/мес)", callback_data=f"payment:subscribe:group:{group_size}:all:month"))
                markup.add(InlineKeyboardButton(f"📦 Все режимы - 3 месяца ({prices['all']['3months']}₽/3 мес)", callback_data=f"payment:subscribe:group:{group_size}:all:3months"))
                markup.add(InlineKeyboardButton(f"📦 Все режимы - год ({prices['all']['year']}₽/год)", callback_data=f"payment:subscribe:group:{group_size}:all:year"))
                markup.add(InlineKeyboardButton(f"📦 Все режимы - навсегда ({prices['all']['lifetime']}₽)", callback_data=f"payment:subscribe:group:{group_size}:all:lifetime"))
                # Проверяем, откуда пришли в тарифы (из действующей подписки или из главного меню)
                back_callback = "payment:active:group:current" if user_payment_state.get(user_id, {}).get('from_active') else f"payment:group_size:{group_size}"
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data=back_callback))
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action == "confirm":
                # Подтверждение платежа - переход к созданию платежа
                state = user_payment_state.get(user_id, {})
                step = state.get('step')
            
                if step == 'confirm_personal':
                    # Подтверждение личной подписки - создаем платеж
                    plan_type = state.get('plan_type')
                    period_type = state.get('period_type')
                    price = state.get('price')
                    is_combined = state.get('is_combined', False)
                    combine_type = state.get('combine_type')
                
                    # Переходим к созданию платежа
                    if is_combined and combine_type == 'pay_now':
                        # Объединенный платеж - создаем платеж на объединенную сумму
                        existing_subs = state.get('existing_subs', [])
                        combined_price = state.get('combined_price', price)
                    
                        # Сохраняем информацию об объединении
                        user_payment_state[user_id] = {
                            'step': 'pay',
                            'subscription_type': 'personal',
                            'plan_type': plan_type,
                            'period_type': period_type,
                            'price': combined_price,
                            'chat_id': chat_id,
                            'telegram_username': call.from_user.username,
                            'is_combined': True,
                            'existing_subs': existing_subs,
                            'combine_type': 'pay_now'
                        }
                    
                        # Вызываем обработчик создания платежа
                        action = f"pay:personal:{plan_type}:{period_type}"
                        # Продолжаем выполнение ниже
                    elif is_combined and combine_type == 'upgrade_to_all':
                        # Переход на "Все режимы"
                        all_price = SUBSCRIPTION_PRICES['personal']['all'].get(period_type, 0)
                        user_payment_state[user_id] = {
                            'step': 'pay',
                            'subscription_type': 'personal',
                            'plan_type': 'all',
                            'period_type': period_type,
                            'price': all_price,
                            'chat_id': chat_id,
                            'telegram_username': call.from_user.username,
                            'is_combined': True,
                            'combine_type': 'upgrade_to_all'
                        }
                    
                        # Вызываем обработчик создания платежа
                        action = f"pay:personal:all:{period_type}"
                        # Продолжаем выполнение ниже
                    else:
                        # Обычный платеж
                        user_payment_state[user_id] = {
                            'step': 'pay',
                            'subscription_type': 'personal',
                            'plan_type': plan_type,
                            'period_type': period_type,
                            'price': price,
                            'chat_id': chat_id,
                            'telegram_username': call.from_user.username
                        }
                    
                        # Вызываем обработчик создания платежа
                        action = f"pay:personal:{plan_type}:{period_type}"
                        # Продолжаем выполнение ниже
                elif step == 'confirm_group':
                    # Подтверждение групповой подписки
                    plan_type = state.get('plan_type')
                    period_type = state.get('period_type')
                    price = state.get('price')
                    group_size = state.get('group_size')
                
                    user_payment_state[user_id] = {
                        'step': 'pay',
                        'subscription_type': 'group',
                        'plan_type': plan_type,
                        'period_type': period_type,
                        'price': price,
                        'group_size': group_size,
                        'chat_id': state.get('chat_id', chat_id),
                        'group_username': state.get('group_username'),
                        'group_title': state.get('group_title')
                    }
                
                    # Вызываем обработчик создания платежа
                    action = f"pay:group:{group_size}:{plan_type}:{period_type}"
                    # Продолжаем выполнение ниже
                else:
                    bot_instance.answer_callback_query(call.id, "Ошибка: неверное состояние", show_alert=True)
                    return
            
                # Продолжаем выполнение - обрабатываем как pay:...
                # Это будет обработано ниже в коде
        
            if action.startswith("combine:"):
                # Обработка объединения подписок
                parts = action.split(":")
                combine_type = parts[1]  # pay_now, add_to_next, upgrade_to_all
            
                if combine_type == "pay_now":
                    # Списать сейчас - создаем платеж на объединенную сумму
                    plan_type = parts[2]
                    period_type = parts[3]
                    state = user_payment_state.get(user_id, {})
                    combined_price = state.get('combined_price', 0)
                    existing_subs = state.get('existing_subs', [])
                
                    # Сохраняем состояние для создания платежа
                    user_payment_state[user_id] = {
                        'step': 'confirm_personal',
                        'subscription_type': 'personal',
                        'plan_type': plan_type,
                        'period_type': period_type,
                        'price': combined_price,
                        'chat_id': chat_id,
                        'telegram_username': call.from_user.username,
                        'is_combined': True,
                        'existing_subs': existing_subs,
                        'combine_type': 'pay_now'
                    }
                
                    # Показываем подтверждение
                    plan_names = {
                        'notifications': 'Уведомления о сериалах',
                        'recommendations': 'Рекомендации',
                        'tickets': 'Билеты'
                    }
                    new_plan_name = plan_names.get(plan_type, plan_type)
                
                    text = "💳 <b>Подтверждение платежа</b>\n\n"
                    text += f"Сумма: <b>{combined_price}₽</b>\n\n"
                    text += "Этот платеж включает:\n"
                    for sub in existing_subs:
                        plan_type_existing = sub.get('plan_type')
                        plan_name = plan_names.get(plan_type_existing, plan_type_existing)
                        text += f"• {plan_name}\n"
                    text += f"• {new_plan_name} (новая)\n\n"
                    text += "Дата следующего списания будет обновлена на сегодня."
                
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("✅ Подтвердить", callback_data="payment:confirm"))
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:tariffs:personal"))
                
                    try:
                        bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    except Exception as e:
                        if "message is not modified" not in str(e):
                            logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                    return
                
                elif combine_type == "add_to_next":
                    # Добавить к следующему списанию - обновляем сумму следующего списания
                    plan_type = parts[2] if len(parts) > 2 else ''
                    period_type = parts[3] if len(parts) > 3 else 'month'
                    state = user_payment_state.get(user_id, {})
                    existing_subs = state.get('existing_subs', [])
                    next_sub = state.get('next_sub')
                
                    if not next_sub:
                        bot_instance.answer_callback_query(call.id, "Ошибка: не найдена подписка для обновления", show_alert=True)
                        return
                
                    # Если переход на "Все режимы", отменяем все старые подписки
                    if plan_type == 'all':
                        from moviebot.database.db_operations import cancel_subscription
                        next_sub_id = next_sub.get('id')
                        
                        # Отменяем все существующие подписки (включая ту, которую обновляем, если она есть в списке)
                        for sub in existing_subs:
                            sub_id = sub.get('id')
                            if sub_id:
                                cancel_subscription(sub_id, user_id)
                                logger.info(f"[PAYMENT] Отменена подписка {sub_id} при переходе на 'Все режимы'")
                        
                        # Обновляем существующую подписку на "Все режимы" (если она есть)
                        all_price = SUBSCRIPTION_PRICES['personal']['all'].get(period_type, 0)
                        from moviebot.database.db_operations import update_subscription_price
                        from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
                        
                        if next_sub_id:
                            # Обновляем существующую подписку
                            update_subscription_price(next_sub_id, all_price)
                            # Обновляем plan_type и period_type
                            conn = get_db_connection()
                            cursor = get_db_cursor()
                            with db_lock:
                                cursor.execute(
                                    'UPDATE subscriptions SET plan_type = %s, period_type = %s, is_active = TRUE WHERE id = %s',
                                    ('all', period_type, next_sub_id)
                                )
                                conn.commit()
                            logger.info(f"[PAYMENT] Обновлена подписка {next_sub_id} на 'Все режимы', цена: {all_price}₽, period_type: {period_type}")
                            
                            next_payment_date = next_sub.get('next_payment_date')
                            if not next_payment_date:
                                next_payment_date = datetime.now(pytz.UTC) + timedelta(days=30)
                        else:
                            # Если next_sub не найден, создаем новую подписку
                            from moviebot.database.db_operations import create_subscription
                            next_payment_date = datetime.now(pytz.UTC) + timedelta(days=30)
                            next_sub_id = create_subscription(
                                chat_id=chat_id,
                                user_id=user_id,
                                subscription_type='personal',
                                plan_type='all',
                                period_type=period_type,
                                price=all_price,
                                telegram_username=call.from_user.username,
                                next_payment_date=next_payment_date
                            )
                            logger.info(f"[PAYMENT] Создана новая подписка {next_sub_id} 'Все режимы' с датой следующего списания {next_payment_date}")
                        
                        text = "✅ <b>Переход на подписку \"Все режимы\"</b>\n\n"
                        text += "Ваши текущие подписки отменены. Подписка \"Все режимы\" будет активирована со следующего списания.\n\n"
                        text += f"💰 Следующее списание: <b>{all_price}₽</b>"
                        if period_type != 'month':
                            period_names = {'3months': '3 месяца', 'year': 'год', 'lifetime': 'навсегда'}
                            period_name = period_names.get(period_type, period_type)
                            text += f" за {period_name}"
                        text += "\n"
                        if isinstance(next_payment_date, datetime):
                            text += f"📅 Дата: {next_payment_date.strftime('%d.%m.%Y')}"
                        else:
                            text += f"📅 Дата: {next_payment_date}"
                    else:
                        # Обычное добавление подписки к следующему списанию
                        combined_price = state.get('combined_price', 0)
                        
                        # Обновляем цену следующего списания
                        from moviebot.database.db_operations import update_subscription_price
                        subscription_id = next_sub.get('id')
                        if subscription_id:
                            update_subscription_price(subscription_id, combined_price)
                            logger.info(f"[PAYMENT] Обновлена цена подписки {subscription_id} на {combined_price}₽")
                        
                        # Создаем новую подписку с той же датой следующего списания
                        from moviebot.database.db_operations import create_subscription
                    
                        next_payment_date = next_sub.get('next_payment_date')
                        if not next_payment_date:
                            next_payment_date = datetime.now(pytz.UTC) + timedelta(days=30)
                        
                        new_subscription_id = create_subscription(
                            chat_id=chat_id,
                            user_id=user_id,
                            subscription_type='personal',
                            plan_type=plan_type,
                            period_type=period_type,
                            price=state.get('new_price', 0),
                            telegram_username=call.from_user.username,
                            next_payment_date=next_payment_date
                        )
                        
                        logger.info(f"[PAYMENT] Создана новая подписка {new_subscription_id} с датой следующего списания {next_payment_date}")
                        
                        plan_names = {
                            'notifications': 'Уведомления о сериалах',
                            'recommendations': 'Рекомендации',
                            'tickets': 'Билеты'
                        }
                        new_plan_name = plan_names.get(plan_type, plan_type)
                        
                        text = "✅ <b>Подписка добавлена</b>\n\n"
                        text += f"Подписка \"{new_plan_name}\" будет добавлена к следующему списанию.\n\n"
                        text += f"💰 Следующее списание: <b>{combined_price}₽</b>\n"
                        if isinstance(next_payment_date, datetime):
                            text += f"📅 Дата: {next_payment_date.strftime('%d.%m.%Y')}"
                        else:
                            text += f"📅 Дата: {next_payment_date}"
                
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:personal"))
                
                    try:
                        bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    except Exception as e:
                        if "message is not modified" not in str(e):
                            logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                    return
                
                elif combine_type == "upgrade_to_all":
                    # Переход на "Все режимы" - отменяем старые, создаем новую
                    period_type = parts[1] if len(parts) > 1 else 'month'
                    state = user_payment_state.get(user_id, {})
                    existing_subs = state.get('existing_subs', [])
                
                    # Отменяем все старые подписки
                    from moviebot.database.db_operations import cancel_subscription
                    for sub in existing_subs:
                        sub_id = sub.get('id')
                        if sub_id:
                            cancel_subscription(sub_id, user_id)
                
                    # Сохраняем состояние для создания новой подписки "Все режимы"
                    all_price = SUBSCRIPTION_PRICES['personal']['all'].get(period_type, 0)
                    user_payment_state[user_id] = {
                        'step': 'confirm_personal',
                        'subscription_type': 'personal',
                        'plan_type': 'all',
                        'period_type': period_type,
                        'price': all_price,
                        'chat_id': chat_id,
                        'telegram_username': call.from_user.username,
                        'is_combined': True,
                        'combine_type': 'upgrade_to_all'
                    }
                
                    # Показываем подтверждение
                    text = "📦 <b>Переход на подписку \"Все режимы\"</b>\n\n"
                    text += "Ваши текущие подписки будут отменены, и будет создана новая подписка \"Все режимы\".\n\n"
                    text += f"💰 Стоимость: <b>{all_price}₽</b>"
                
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("✅ Подтвердить", callback_data="payment:confirm"))
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:tariffs:personal"))
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action.startswith("modify:"):
                logger.info(f"[PAYMENT MODIFY] Получен callback modify: action={action}, user_id={user_id}")
                try:
                    bot_instance.answer_callback_query(call.id)
                except:
                    pass
                
                parts = action.split(":")
                subscription_id_str = parts[1] if len(parts) > 1 else None
                logger.info(f"[PAYMENT MODIFY] subscription_id_str={subscription_id_str}")
                
                # Определяем тип подписки по контексту чата
                is_private = call.message.chat.type == 'private'
                subscription_type = 'personal' if is_private else 'group'
                
                # ─── modify:all ─── только для personal в личке (добавление новых планов)
                if subscription_id_str == "all" and subscription_type == 'personal':
                    logger.info(f"[PAYMENT MODIFY] Обработка modify:all для user_id={user_id}")
                    from moviebot.database.db_operations import get_user_personal_subscriptions
                    
                    all_subs = get_user_personal_subscriptions(user_id)
                    
                    # Фильтр активных и уникальных plan_type
                    active_subs = []
                    seen_plan_types = set()
                    now = datetime.now(pytz.UTC)
                    existing_plan_types = []
                    
                    for sub in all_subs:
                        expires_at = sub.get('expires_at')
                        plan_type = sub.get('plan_type')
                        
                        is_active = False
                        if not expires_at:
                            is_active = True
                        elif isinstance(expires_at, datetime):
                            if expires_at.tzinfo is None:
                                expires_at = pytz.UTC.localize(expires_at)
                            if expires_at.tzinfo != pytz.UTC:
                                expires_at = expires_at.astimezone(pytz.UTC)
                            is_active = expires_at > now
                        else:
                            try:
                                if isinstance(expires_at, str):
                                    expires_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                                    if expires_dt.tzinfo is None:
                                        expires_dt = pytz.UTC.localize(expires_dt)
                                    if expires_dt.tzinfo != pytz.UTC:
                                        expires_dt = expires_dt.astimezone(pytz.UTC)
                                    is_active = expires_dt > now
                                else:
                                    is_active = True
                            except:
                                is_active = True
                        
                        if is_active and plan_type and plan_type not in seen_plan_types:
                            active_subs.append(sub)
                            seen_plan_types.add(plan_type)
                            existing_plan_types.append(plan_type)
                    
                    plan_names = {
                        'notifications': 'Уведомления о сериалах',
                        'recommendations': 'Рекомендации',
                        'tickets': 'Билеты',
                        'all': 'Все режимы'
                    }
                    
                    text = "✏️ <b>Изменить подписку</b>\n\n"
                    text += "Выберите подписку, которую хотите добавить:\n\n"
                    
                    markup = InlineKeyboardMarkup(row_width=1)
                    
                    if 'notifications' not in existing_plan_types:
                        markup.add(InlineKeyboardButton("🔔 Уведомления о сериалах", callback_data="payment:subscribe:personal:notifications:month"))
                    if 'recommendations' not in existing_plan_types:
                        markup.add(InlineKeyboardButton("🎯 Персональные рекомендации", callback_data="payment:subscribe:personal:recommendations:month"))
                    if 'tickets' not in existing_plan_types:
                        markup.add(InlineKeyboardButton("🎫 Билеты в кино", callback_data="payment:subscribe:personal:tickets:month"))
                    if 'all' not in existing_plan_types:
                        markup.add(InlineKeyboardButton("📦 Все режимы", callback_data="payment:subscribe:personal:all:month"))
                    
                    if len(existing_plan_types) >= 3 or 'all' in existing_plan_types:
                        text = "✏️ <b>Изменить подписку</b>\n\n"
                        if 'all' in existing_plan_types:
                            text += "У вас уже подключена подписка \"Все режимы\", которая включает все функции.\n\n"
                        else:
                            text += "У вас уже подключены все доступные подписки.\n\n"
                        text += "Вы можете отменить одну из подписок, чтобы добавить другую."
                    
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:personal"))
                    
                    try:
                        bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    except Exception as e:
                        if "message is not modified" not in str(e):
                            logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                    return
                
                # ─── modify:<id> ─── конкретная подписка (personal или group)
                if subscription_id_str and subscription_id_str.isdigit():
                    subscription_id = int(subscription_id_str)
                    
                    from moviebot.database.db_operations import get_subscription_by_id
                    sub = get_subscription_by_id(subscription_id)
                    
                    if not sub or sub.get('user_id') != user_id or not sub.get('is_active', True):
                        bot_instance.answer_callback_query(call.id, "Подписка не найдена или не активна", show_alert=True)
                        return
                    
                    plan_type = sub.get('plan_type', 'all')
                    period_type = sub.get('period_type', 'month')
                    group_size = sub.get('group_size') if subscription_type == 'group' else None
                    
                    # Если максимальная — ничего не предлагаем
                    if plan_type == 'all' and period_type == 'lifetime':
                        text = "✅ <b>У вас куплен весь функционал бота</b>\n\n"
                        text += "📦 Пакетная подписка - Все режимы\n"
                        text += "⏰ Период: навсегда\n\n"
                        text += "Дополнительные опции недоступны."
                        
                        markup = InlineKeyboardMarkup(row_width=1)
                        back_callback = "payment:active:personal" if subscription_type == 'personal' else "payment:active:group:current"
                        markup.add(InlineKeyboardButton("◀️ Назад", callback_data=back_callback))
                        
                        try:
                            bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                        except Exception as e:
                            if "message is not modified" not in str(e):
                                logger.error(f"[PAYMENT MODIFY] Ошибка: {e}")
                        return
                    
                    # Обычная подписка — предлагаем изменить
                    # Определяем названия тарифов
                    plan_names = {
                        'notifications': 'Уведомления о сериалах',
                        'recommendations': 'Рекомендации',
                        'tickets': 'Билеты',
                        'all': 'Все режимы'
                    }
                    
                    # Определяем названия периодов
                    period_names = {
                        'month': 'месяц',
                        '3months': '3 месяца',
                        'year': 'год',
                        'lifetime': 'навсегда'
                    }
                    
                    plan_name = plan_names.get(plan_type, plan_type)
                    period_name = period_names.get(period_type, period_type)
                    
                    text = f"✏️ <b>Изменить { 'личную' if subscription_type == 'personal' else 'групповую' } подписку</b>\n\n"
                    text += f"Текущий тариф: <b>{plan_name}</b>\n"
                    text += f"Период: <b>{period_name}</b>\n"
                    if group_size:
                        text += f"Размер: <b>{group_size} человек</b>\n"
                    text += "\nВыберите действие:\n"
                    
                    markup = InlineKeyboardMarkup(row_width=1)
                    
                    # Главная кнопка — перейти к тарифам/периодам
                    tariffs_callback = f"payment:tariffs:{subscription_type}:{subscription_id}"
                    markup.add(InlineKeyboardButton("💰 Изменить тариф/период", callback_data=tariffs_callback))
                    
                    # Для группы — возможность докупить другие тарифы
                    if subscription_type == 'group' and plan_type != 'all':
                        group_size_str = str(group_size) if group_size else '2'
                        # Определяем, какие тарифы отсутствуют
                        missing_functions = []
                        if plan_type != 'notifications':
                            missing_functions.append(('notifications', '🔔 Уведомления о сериалах', SUBSCRIPTION_PRICES['group'][group_size_str]['notifications'].get('month', 0)))
                        if plan_type != 'recommendations':
                            missing_functions.append(('recommendations', '🎯 Рекомендации', SUBSCRIPTION_PRICES['group'][group_size_str]['recommendations'].get('month', 0)))
                        if plan_type != 'tickets':
                            missing_functions.append(('tickets', '🎫 Билеты', SUBSCRIPTION_PRICES['group'][group_size_str]['tickets'].get('month', 0)))
                        
                        # Предлагаем докупить недостающие тарифы или обновить до "Все режимы"
                        if missing_functions:
                            current_month_price = SUBSCRIPTION_PRICES['group'][group_size_str][plan_type].get('month', 0)
                            all_month_price = SUBSCRIPTION_PRICES['group'][group_size_str]['all'].get('month', 0)
                            upgrade_price = all_month_price - current_month_price
                            if upgrade_price > 0:
                                markup.add(InlineKeyboardButton(f"📦 Все режимы (+{upgrade_price}₽/мес)", callback_data=f"payment:upgrade_plan:{subscription_id}:all"))
                            
                            # Предлагаем докупить отдельные функции (если их 1-2)
                            if len(missing_functions) <= 2:
                                for func_type, func_name, func_price in missing_functions:
                                    add_price = func_price - current_month_price if func_price > current_month_price else func_price
                                    if add_price > 0:
                                        markup.add(InlineKeyboardButton(f"{func_name} (+{add_price}₽/мес)", callback_data=f"payment:upgrade_plan:{subscription_id}:{func_type}"))
                    
                    # Для группы — расширение размера (expand)
                    if subscription_type == 'group' and group_size and group_size < 10:
                        next_size = 5 if group_size == 2 else 10
                        plan_type_sub = sub.get('plan_type')
                        period_type_sub = sub.get('period_type')
                        current_price = SUBSCRIPTION_PRICES['group'][str(group_size)][plan_type_sub].get(period_type_sub, 0)
                        if next_size == 5:
                            price_5 = SUBSCRIPTION_PRICES['group']['5'][plan_type_sub].get(period_type_sub, 0)
                            diff_5 = price_5 - current_price
                            from moviebot.database.db_operations import get_user_personal_subscriptions
                            personal_subs = get_user_personal_subscriptions(user_id)
                            if personal_subs:
                                diff_5 = int(diff_5 * 0.5)
                            markup.add(InlineKeyboardButton(f"📈 Расширить до 5 (+{diff_5}₽)", callback_data=f"payment:expand:5:{subscription_id}"))
                            price_10 = SUBSCRIPTION_PRICES['group']['10'][plan_type_sub].get(period_type_sub, 0)
                            diff_10 = price_10 - current_price
                            if personal_subs:
                                diff_10 = int(price_10 * 0.5) - current_price
                            markup.add(InlineKeyboardButton(f"📈 Расширить до 10 (+{diff_10}₽)", callback_data=f"payment:expand:10:{subscription_id}"))
                        elif next_size == 10:
                            price_10 = SUBSCRIPTION_PRICES['group']['10'][plan_type_sub].get(period_type_sub, 0)
                            diff_10 = price_10 - current_price
                            from moviebot.database.db_operations import get_user_personal_subscriptions
                            personal_subs = get_user_personal_subscriptions(user_id)
                            if personal_subs:
                                diff_10 = int(price_10 * 0.5) - current_price
                            markup.add(InlineKeyboardButton(f"📈 Расширить до 10 (+{diff_10}₽)", callback_data=f"payment:expand:10:{subscription_id}"))
                    
                    markup.add(InlineKeyboardButton("❌ Отменить подписку", callback_data=f"payment:cancel:{subscription_id}"))
                    back_callback = "payment:active:personal" if subscription_type == 'personal' else "payment:active:group"
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data=back_callback))
                    
                    try:
                        bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    except Exception as e:
                        if "message is not modified" not in str(e):
                            logger.error(f"[PAYMENT MODIFY] Ошибка редактирования: {e}")
                    return
                
                # Если дошли сюда — неизвестный формат
                bot_instance.answer_callback_query(call.id, "Неизвестный формат изменения подписки", show_alert=True)




            if action.startswith("subscribe:"):
                # Обработка подписки
                parts = action.split(":")
                sub_type = parts[1]  # personal или group
            
                # Инициализируем переменные группы для всех случаев
                group_chat_id = None
                group_username = None
                group_title = None
                group_size = None
            
                # Правильный парсинг для групп: payment:subscribe:group:2:all:month или payment:subscribe:group:2:all:month:chat_id
                # Для личных: payment:subscribe:personal:all:month
                if sub_type == 'group' and len(parts) >= 5:
                    group_size_str = parts[2]
                    group_size = group_size_str  # Keep as string for SUBSCRIPTION_PRICES keys
                    plan_type = parts[3] if len(parts) > 3 else ''
                    period_type = parts[4] if len(parts) > 4 else ''
                
                    # Получаем информацию о группе из состояния
                    state = user_payment_state.get(user_id, {})
                    group_chat_id = state.get('group_chat_id')
                    group_username = state.get('group_username')
                    group_title = state.get('group_title')
                
                    # Если есть chat_id в конце (часть 5 или 6), используем его
                    if len(parts) >= 6:
                        try:
                            group_chat_id_from_callback = int(parts[5])
                            # Если группа не выбрана в состоянии, используем chat_id из callback
                            if not group_chat_id:
                                group_chat_id = group_chat_id_from_callback
                        except (ValueError, IndexError):
                            pass
                
                    # Если группа не выбрана, используем текущий чат (если это группа)
                    if not group_chat_id:
                        if not is_private:
                            group_chat_id = chat_id
                            try:
                                chat_info = bot_instance.get_chat(chat_id)
                                group_username = chat_info.username
                                group_title = chat_info.title
                            except:
                                pass
                        else:
                            # В личке без выбранной группы - просим выбрать
                            from moviebot.database.db_operations import get_user_groups
                            user_groups = get_user_groups(user_id, bot)
                            if not user_groups:
                                bot_instance.answer_callback_query(call.id, "Сначала выберите группу", show_alert=True)
                                return
                        
                            # Показываем выбор группы
                            text = f"👥 <b>Выберите группу для подписки на {group_size} участников</b>\n\n"
                            text += "Выберите группу из списка:"
                        
                            markup = InlineKeyboardMarkup(row_width=1)
                            for group in user_groups[:10]:
                                group_title = group.get('title', f"Группа {group.get('chat_id')}")
                                group_username = group.get('username')
                                if group_username:
                                    button_text = f"📍 {group_title} (@{group_username})"
                                else:
                                    button_text = f"📍 {group_title}"
                                if len(button_text) > 50:
                                    button_text = button_text[:47] + "..."
                                markup.add(InlineKeyboardButton(
                                    button_text,
                                    callback_data=f"payment:select_group:{group_size}:{group.get('chat_id')}"
                                ))
                            markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:group_size:{group_size}"))
                        
                            try:
                                bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                            except Exception as e:
                                if "message is not modified" not in str(e):
                                    logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                            return
                else:
                    group_size = None
                    plan_type = parts[2] if len(parts) > 2 else ''
                    period_type = parts[3] if len(parts) > 3 else ''
                    group_chat_id = None
                    group_username = None
                    group_title = None
            
                # Для пользователя 301810276 разрешаем оплату всегда
                is_owner = (user_id == 301810276)
            
                # Проверяем, есть ли уже подписка с этими функциями (только для не-владельца)
                if not is_owner:
                    from moviebot.database.db_operations import get_active_subscription, has_subscription_feature, get_active_group_subscription
                
                    # Для групповых подписок проверяем существующую подписку группы
                    if sub_type == 'group' and group_chat_id:
                        # Получаем информацию о группе для проверки подписки
                        try:
                            if not group_username:
                                chat_info = bot_instance.get_chat(group_chat_id)
                                group_username = chat_info.username
                        except:
                            pass
                    
                        if group_username:
                            existing_group_sub = get_active_group_subscription(group_username)
                            if existing_group_sub:
                                existing_plan_type = existing_group_sub.get('plan_type', '')
                                existing_price = existing_group_sub.get('price', 0)
                            
                                # Получаем цену выбранного тарифа
                                try:
                                    if plan_type == 'all':
                                        selected_price = SUBSCRIPTION_PRICES['group'][group_size]['all'].get(period_type, 0)
                                    else:
                                        # Для отдельных функций только месячная подписка
                                        if period_type == 'month':
                                            selected_price = SUBSCRIPTION_PRICES['group'][group_size][plan_type].get('month', 0)
                                        else:
                                            selected_price = 0
                                except Exception as e:
                                    logger.error(f"[PAYMENT] Ошибка получения цены: {e}")
                                    selected_price = 0
                            
                                # Проверяем, если существующая подписка покрывает выбранную функцию или имеет более высокий тариф
                                covers_selected = False
                                if existing_plan_type == 'all':
                                    covers_selected = True
                                elif existing_plan_type == plan_type:
                                    # Если тот же тип подписки, проверяем цену
                                    if period_type == 'month':
                                        existing_month_price = SUBSCRIPTION_PRICES['group'][group_size].get(existing_plan_type, {}).get('month', 0)
                                        if existing_month_price >= selected_price:
                                            covers_selected = True
                            
                                if covers_selected:
                                    plan_names = {
                                        'notifications': '🔔 Уведомления о сериалах',
                                        'recommendations': '🎯 Персональные рекомендации',
                                        'tickets': '🎫 Билеты в кино',
                                        'all': '📦 Все режимы'
                                    }
                                    existing_plan_name = plan_names.get(existing_plan_type, existing_plan_type)
                                    selected_plan_name = plan_names.get(plan_type, plan_type)
                                
                                    text = f"ℹ️ <b>Информация о подписке</b>\n\n"
                                    text += f"В группе <b>{group_title or 'группе'}</b> уже активна подписка:\n"
                                    text += f"<b>{existing_plan_name}</b> ({existing_price}₽)\n\n"
                                    text += f"Выбранный тариф <b>{selected_plan_name}</b> ({selected_price}₽) "
                                    text += "уже включен в текущую подписку или имеет меньшую стоимость.\n\n"
                                    text += "Если вы хотите изменить подписку, сначала отмените текущую."
                                
                                    markup = InlineKeyboardMarkup(row_width=1)
                                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:select_group:{group_size}:{group_chat_id}"))
                                
                                    try:
                                        bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                                    except Exception as e:
                                        if "message is not modified" not in str(e):
                                            logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                                    return
                
                    # Проверяем, какие функции уже есть (для личных подписок)
                    has_notifications = has_subscription_feature(chat_id, user_id, 'notifications')
                    has_recommendations = has_subscription_feature(chat_id, user_id, 'recommendations')
                    has_tickets = has_subscription_feature(chat_id, user_id, 'tickets')
                
                    # Определяем, нужно ли показывать предупреждение (только для личных подписок)
                    if sub_type == 'personal':
                        need_expansion = False
                        expansion_text = ""
                    
                        if plan_type == 'notifications' and has_notifications:
                            need_expansion = True
                            expansion_text = "🔔 Уведомления о сериалах уже включены в вашу подписку."
                        elif plan_type == 'recommendations' and has_recommendations:
                            need_expansion = True
                            expansion_text = "🎯 Персональные рекомендации уже включены в вашу подписку."
                        elif plan_type == 'tickets' and has_tickets:
                            need_expansion = True
                            expansion_text = "🎫 Билеты в кино уже включены в вашу подписку."
                        elif plan_type == 'all' and has_notifications and has_recommendations and has_tickets:
                            need_expansion = True
                            expansion_text = "📦 Все режимы уже включены в вашу подписку."
                    
                        if need_expansion:
                            text = "✅ <b>Ваша подписка оформлена, но вы можете ее расширить:</b>\n\n"
                            text += expansion_text + "\n\n"
                            text += "💡 <b>Доступные варианты расширения:</b>\n\n"
                        
                            # Предлагаем варианты расширения
                            expansion_options = []
                            if not has_notifications:
                                expansion_options.append(("🔔 Уведомления о сериалах", "payment:subscribe:personal:notifications:month"))
                            if not has_recommendations:
                                expansion_options.append(("🎯 Персональные рекомендации", "payment:subscribe:personal:recommendations:month"))
                            if not has_tickets:
                                expansion_options.append(("🎫 Билеты в кино", "payment:subscribe:personal:tickets:month"))
                            if not (has_notifications and has_recommendations and has_tickets):
                                expansion_options.append(("📦 Все режимы", "payment:subscribe:personal:all:month"))
                        
                            markup = InlineKeyboardMarkup(row_width=1)
                            for option_text, callback_data in expansion_options:
                                markup.add(InlineKeyboardButton(option_text, callback_data=callback_data))
                            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:tariffs:personal"))
                        
                            try:
                                bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                            except Exception as e:
                                if "message is not modified" not in str(e):
                                    logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                            return
            
                # Проверяем существующие подписки для личных подписок
                if sub_type == 'personal' and not is_owner:
                    from moviebot.database.db_operations import get_user_personal_subscriptions
                    existing_subs = get_user_personal_subscriptions(user_id)
                
                    # Фильтруем только активные подписки и убираем дубликаты по plan_type
                    active_subs = []
                    seen_plan_types = set()
                    # Используем UTC для сравнения, чтобы избежать проблем с timezone
                    now = datetime.now(pytz.UTC)
                
                    for sub in existing_subs:
                        expires_at = sub.get('expires_at')
                        plan_type = sub.get('plan_type')
                    
                        # Проверяем, что подписка активна
                        is_active = False
                        if not expires_at:
                            # Если нет expires_at, считаем подписку активной (lifetime)
                            is_active = True
                        elif isinstance(expires_at, datetime):
                            # Приводим expires_at к aware datetime, если он naive
                            if expires_at.tzinfo is None:
                                expires_at = pytz.UTC.localize(expires_at)
                            # Приводим к UTC для корректного сравнения
                            if expires_at.tzinfo != pytz.UTC:
                                expires_at = expires_at.astimezone(pytz.UTC)
                            is_active = expires_at > now
                        else:
                            # Если expires_at - это строка или другой тип, пытаемся преобразовать
                            try:
                                if isinstance(expires_at, str):
                                    expires_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                                    # Убеждаемся, что datetime aware
                                    if expires_dt.tzinfo is None:
                                        expires_dt = pytz.UTC.localize(expires_dt)
                                    # Приводим к UTC для корректного сравнения
                                    if expires_dt.tzinfo != pytz.UTC:
                                        expires_dt = expires_dt.astimezone(pytz.UTC)
                                    is_active = expires_dt > now
                                else:
                                    is_active = True  # Если не можем проверить, считаем активной
                            except:
                                is_active = True
                    
                        # Добавляем только активные и уникальные по plan_type
                        if is_active and plan_type and plan_type not in seen_plan_types:
                            active_subs.append(sub)
                            seen_plan_types.add(plan_type)
                
                    if active_subs:
                        # Выносим общую подготовку данных один раз
                        existing_plan_types = [sub.get('plan_type') for sub in active_subs]
                        has_all = 'all' in existing_plan_types
                        
                        # Считаем сумму и имена существующих
                        plan_names_short = {
                            'notifications': 'Уведомления',
                            'recommendations': 'Рекомендации',
                            'tickets': 'Билеты'
                        }
                        existing_sub_names = [plan_names_short.get(pt, pt) for pt in existing_plan_types]
                        total_existing_price = sum(sub.get('price', 0) for sub in active_subs)
                        
                        # Ищем ближайшее следующее списание
                        next_payment_date = None
                        next_sub = None
                        for sub in active_subs:
                            npd = sub.get('next_payment_date')
                            if npd:
                                if not next_payment_date or (isinstance(npd, datetime) and isinstance(next_payment_date, datetime) and npd < next_payment_date):
                                    next_payment_date = npd
                                    next_sub = sub
                        
                        # Цена новой подписки (для month всегда, для других — берём month эквивалент)
                        new_price = SUBSCRIPTION_PRICES['personal'][plan_type].get(period_type, 0)
                        if period_type != 'month':
                            new_price = SUBSCRIPTION_PRICES['personal'][plan_type].get('month', 0)  # fallback
                        
                        combined_price = total_existing_price + new_price
                        
                        # Сохраняем в состояние сразу
                        if user_id not in user_payment_state:
                            user_payment_state[user_id] = {}
                        state = user_payment_state[user_id]
                        state['existing_subs'] = active_subs
                        state['total_existing_price'] = total_existing_price
                        state['new_plan_type'] = plan_type
                        state['new_period_type'] = period_type
                        state['new_price'] = new_price
                        state['next_sub'] = next_sub
                        state['next_payment_date'] = next_payment_date
                        
                        if has_all:
                            text = "⚠️ <b>У вас уже есть подписка \"Все режимы\"</b>\n\n"
                            text += "Вы не можете добавить дополнительные подписки к пакетной.\n\n"
                            text += "Если хотите изменить — сначала отмените текущую."
                            markup = InlineKeyboardMarkup(row_width=1)
                            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:tariffs:personal"))
                            bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                            return
                        
                        elif plan_type == 'all':
                            # Пытаемся добавить пакетную, когда есть отдельные
                            text = "📦 <b>Оформление подписки \"Все режимы\"</b>\n\n"
                            text += "⚠️ <b>У вас уже есть активные подписки:</b>\n"
                            for name in existing_sub_names:
                                text += f"• {name}\n"
                            
                            text += f"\n💰 Текущие: {total_existing_price}₽/мес\n"
                            text += f"💰 \"Все режимы\": {new_price}₽"
                            if period_type != 'month':
                                period_names = {'3months': '3 мес', 'year': 'год', 'lifetime': 'навсегда'}
                                text += f" за {period_names.get(period_type, period_type)}"
                            text += "\n\n"
                            
                            if period_type == 'month':
                                diff = new_price - total_existing_price
                                if diff > 0:
                                    text += f"Доплата: {diff}₽/мес\n"
                                elif diff < 0:
                                    text += f"Экономия: {abs(diff)}₽/мес\n"
                            
                            text += "Выберите способ:\n\n"
                            text += "1️⃣ Отменить текущие и оформить новую сразу\n"
                            
                            markup = InlineKeyboardMarkup(row_width=1)
                            markup.add(InlineKeyboardButton("1️⃣ Отменить сейчас и оформить", callback_data=f"payment:combine:upgrade_to_all:{period_type}"))
                            
                            if period_type == 'month' and next_payment_date:
                                text += f"2️⃣ Увеличить со следующего списания ({new_price}₽) — дата: {next_payment_date.strftime('%d.%m.%Y') if isinstance(next_payment_date, datetime) else next_payment_date}\n"
                                markup.add(InlineKeyboardButton("2️⃣ Увеличить со следующего", callback_data=f"payment:combine:add_to_next:all:{period_type}"))
                            
                            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:tariffs:personal"))
                            
                            bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                            return
                        
                        elif len(existing_plan_types) == 2 and plan_type != 'all':
                            # 2 из 3 отдельных — предлагаем пакетную
                            text = f"⚠️ У вас уже \"{', '.join(existing_sub_names)}\"\n\n"
                            text += "Оформите \"Все режимы\" для полного доступа.\n"
                            text += "Текущие подписки будут отменены автоматически."
                            
                            markup = InlineKeyboardMarkup(row_width=1)
                            all_month = SUBSCRIPTION_PRICES['personal']['all'].get('month', 0)
                            all_3m = SUBSCRIPTION_PRICES['personal']['all'].get('3months', 0)
                            all_life = SUBSCRIPTION_PRICES['personal']['all'].get('lifetime', 0)
                            markup.add(InlineKeyboardButton(f"Все режимы ({all_month}₽/мес)", callback_data="payment:subscribe:personal:all:month"))
                            if all_3m > 0:
                                markup.add(InlineKeyboardButton(f"Все режимы ({all_3m}₽/3 мес)", callback_data="payment:subscribe:personal:all:3months"))
                            if all_life > 0:
                                markup.add(InlineKeyboardButton(f"Все режимы ({all_life}₽ навсегда)", callback_data="payment:subscribe:personal:all:lifetime"))
                            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:tariffs:personal"))
                            
                            bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                            return
                        
                        elif plan_type in existing_plan_types:
                            # Уже есть такой план
                            plan_name = plan_names_short.get(plan_type, plan_type)
                            text = f"⚠️ У вас уже есть \"{plan_name}\"\n\nОтмените текущую, если хотите изменить."
                            markup = InlineKeyboardMarkup(row_width=1)
                            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:tariffs:personal"))
                            bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                            return
                        
                        else:
                            # Обычное добавление — объединение
                            text = "💎 <b>Объединение подписок</b>\n\n"
                            text += f"Текущие: {', '.join(existing_sub_names)}\n"
                            text += f"Добавляем: {plan_names_short.get(plan_type, plan_type)}\n\n"
                            text += f"Текущие: {total_existing_price}₽/мес\n"
                            text += f"Новая: {new_price}₽/мес\n"
                            text += f"<b>Итого: {combined_price}₽/мес</b>\n\n"
                            
                            if next_payment_date:
                                text += f"Следующее списание: {next_payment_date.strftime('%d.%m.%Y') if isinstance(next_payment_date, datetime) else next_payment_date}\n\n"
                            
                            text += "Выберите оплату:"
                            
                            markup = InlineKeyboardMarkup(row_width=1)
                            markup.add(InlineKeyboardButton(f"💳 Списать сейчас ({combined_price}₽)", callback_data=f"payment:combine:pay_now:{plan_type}:{period_type}"))
                            if next_payment_date:
                                markup.add(InlineKeyboardButton(f"📅 Добавить к следующему ({combined_price}₽)", callback_data=f"payment:combine:add_to_next:{plan_type}:{period_type}"))
                            markup.add(InlineKeyboardButton("📦 Все режимы", callback_data="payment:combine:upgrade_to_all:month"))
                            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:tariffs:personal"))
                            
                            bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                            return
                            
                # Показываем подробное описание тарифа
                if sub_type == 'personal':
                    if plan_type == 'notifications':
                        text = "🔔 <b>Уведомления о сериалах</b>\n\n"
                        text += "💎 <b>Что входит в подписку:</b>\n"
                        text += "• Автоматические уведомления о выходе новых серий\n"
                        text += "• Настройка времени уведомлений (будни/выходные)\n"
                        text += "• Персонализированные напоминания для каждого сериала\n"
                        text += "• Отслеживание прогресса просмотра сезонов\n\n"
                        text += "❌ <b>Без подписки:</b>\n"
                        text += "• Уведомления о сериалах недоступны\n"
                        text += "• Раздел \"Настройка уведомлений\" заблокирован\n\n"
                        text += f"💰 <b>Стоимость:</b> {SUBSCRIPTION_PRICES['personal']['notifications']['month']}₽/мес"
                    elif plan_type == 'recommendations':
                        text = "🎯 <b>Персональные рекомендации</b>\n\n"
                        text += "Вы сможете не просто найти фильм из ранее отложенных к просмотру, но и получить рекомендацию, основываясь на ваших личных или групповых оценках. Вы сможете найти новый фильм, который вам точно подойдет!\n\n"
                        text += "💎 <b>Что входит в подписку:</b>\n"
                        text += "• Режим \"По оценкам в базе\" — рекомендации по оценкам фильмов, добавленных в базу чата или группы\n"
                        text += "• Режим \"Рандом по Кинопоиску\" — случайный фильм из Кинопоиска по фильтрам\n"
                        text += "• Режим рандомайзера \"По моим оценкам\" — рекомендации по оценкам из Кинопоиска\n"
                        text += "• Импорт базы из Кинопоиска\n\n"
                        text += "❌ <b>Без подписки:</b>\n"
                        text += "• Режимы \"По моим оценкам\", \"По оценкам в базе\" и \"Рандом по Кинопоиску\" заблокированы\n"
                        text += "• Импорт базы из Кинопоиска недоступен\n\n"
                        text += f"💰 <b>Стоимость:</b> {SUBSCRIPTION_PRICES['personal']['recommendations']['month']}₽/мес"
                    elif plan_type == 'tickets':
                        text = "🎫 <b>Билеты в кино</b>\n\n"
                        text += "Вы сможете добавлять билеты на сеансы и любые другие мероприятия в бот, и они всегда будут в доступе по одной кнопке. В день мероприятия вам придет уведомление, а за непосредственно перед мероприятием бот пришлет билеты, чтобы не пришлось их искать на входе. Мы не храним и не обрабатываем файлы.\n\n"
                        text += "💎 <b>Что входит в подписку:</b>\n"
                        text += "• Добавление билетов на сеансы и мероприятия\n"
                        text += "• Настраиваемые уведомления с билетами перед мероприятием\n\n"
                        text += "❌ <b>Без подписки:</b>\n"
                        text += "• Добавление билетов недоступно\n"
                        text += "• Настройка уведомлений с билетами заблокирована\n\n"
                        text += f"💰 <b>Стоимость:</b> {SUBSCRIPTION_PRICES['personal']['tickets']['month']}₽/мес"
                    elif plan_type == 'all':
                        period_names = {
                            'month': 'месяц',
                            '3months': '3 месяца',
                            'year': 'год',
                            'lifetime': 'навсегда'
                        }
                        period_name = period_names.get(period_type, period_type)
                        price = SUBSCRIPTION_PRICES['personal']['all'][period_type]
                        text = "📦 <b>Все режимы</b>\n\n"
                        text += "💎 <b>Что входит в подписку:</b>\n\n"
                        text += "🔔 <b>Уведомления о сериалах:</b>\n"
                        text += "• Автоматические уведомления о выходе новых серий\n"
                        text += "• Настройка времени уведомлений\n\n"
                        text += "🎯 <b>Персональные рекомендации:</b>\n"
                        text += "• Режим \"По оценкам в базе\" — рекомендации по оценкам фильмов, добавленных в базу чата или группы\n"
                        text += "• Режим \"Рандом по Кинопоиску\" — случайный фильм из Кинопоиска по фильтрам\n"
                        text += "• Режим рандомайзера \"По моим оценкам\" — рекомендации по оценкам из Кинопоиска\n"
                        text += "• Импорт базы из Кинопоиска\n\n"
                        text += "🎫 <b>Билеты в кино:</b>\n"
                        text += "• Добавление билетов на сеансы и мероприятия\n"
                        text += "• Настраиваемые уведомления с билетами перед мероприятием\n\n"
                        text += f"💰 <b>Стоимость:</b> {price}₽ за {period_name}"
                else:  # group
                    if plan_type == 'notifications':
                        text = f"🔔 <b>Уведомления о сериалах (на {group_size} участников)</b>\n\n"
                        text += "💎 <b>Что входит в подписку:</b>\n"
                        text += "• Автоматические уведомления о выходе новых серий для всех участников\n"
                        text += "• Настройка времени уведомлений (будни/выходные)\n"
                        text += "• Персонализированные напоминания для каждого сериала\n"
                        text += "• Отслеживание прогресса просмотра сезонов\n\n"
                        text += "❌ <b>Без подписки:</b>\n"
                        text += "• Уведомления о сериалах недоступны\n"
                        text += "• Раздел \"Настройка уведомлений\" заблокирован\n\n"
                        base_price = SUBSCRIPTION_PRICES['group'][group_size]['notifications']['month']
                        price = calculate_discounted_price(user_id, 'group', 'notifications', 'month', group_size)
                        text += f"💰 <b>Стоимость:</b> {price}₽/мес"
                        if price < base_price:
                            text += f" <s>(было {base_price}₽)</s>"
                    elif plan_type == 'recommendations':
                        text = f"🎯 <b>Персональные рекомендации (на {group_size} участников)</b>\n\n"
                        text += "Вы сможете не просто найти фильм из ранее отложенных к просмотру, но и получить рекомендацию, основываясь на ваших личных или групповых оценках. Вы сможете найти новый фильм, который вам точно подойдет!\n\n"
                        text += "💎 <b>Что входит в подписку:</b>\n"
                        text += "• Режим \"По оценкам в базе\" — рекомендации по оценкам фильмов, добавленных в базу чата или группы\n"
                        text += "• Режим \"Рандом по Кинопоиску\" — случайный фильм из Кинопоиска по фильтрам\n"
                        text += "• Режим рандомайзера \"По моим оценкам\" — рекомендации по оценкам из Кинопоиска\n"
                        text += "• Импорт базы из Кинопоиска для всех участников\n\n"
                        text += "❌ <b>Без подписки:</b>\n"
                        text += "• Режимы \"По моим оценкам\", \"По оценкам в базе\" и \"Рандом по Кинопоиску\" заблокированы\n"
                        text += "• Импорт базы из Кинопоиска недоступен\n\n"
                        base_price = SUBSCRIPTION_PRICES['group'][group_size]['recommendations']['month']
                        price = calculate_discounted_price(user_id, 'group', 'recommendations', 'month', group_size)
                        text += f"💰 <b>Стоимость:</b> {price}₽/мес"
                        if price < base_price:
                            text += f" <s>(было {base_price}₽)</s>"
                    elif plan_type == 'tickets':
                        text = f"🎫 <b>Билеты в кино (на {group_size} участников)</b>\n\n"
                        text += "Вы сможете добавлять билеты на сеансы и любые другие мероприятия в бот, и они всегда будут в доступе по одной кнопке. В день мероприятия вам придет уведомление, а за непосредственно перед мероприятием бот пришлет билеты, чтобы не пришлось их искать на входе. Мы не храним и не обрабатываем файлы.\n\n"
                        text += "💎 <b>Что входит в подписку:</b>\n"
                        text += "• Добавление билетов на сеансы и мероприятия для всех участников\n"
                        text += "• Настраиваемые уведомления с билетами перед мероприятием\n\n"
                        text += "❌ <b>Без подписки:</b>\n"
                        text += "• Добавление билетов недоступно\n"
                        text += "• Настройка уведомлений с билетами заблокирована\n\n"
                        base_price = SUBSCRIPTION_PRICES['group'][group_size]['tickets']['month']
                        price = calculate_discounted_price(user_id, 'group', 'tickets', 'month', group_size)
                        text += f"💰 <b>Стоимость:</b> {price}₽/мес"
                        if price < base_price:
                            text += f" <s>(было {base_price}₽)</s>"
                    elif plan_type == 'all':
                        period_names = {
                            'month': 'месяц',
                            '3months': '3 месяца',
                            'year': 'год',
                            'lifetime': 'навсегда'
                        }
                        period_name = period_names.get(period_type, period_type)
                        base_price = SUBSCRIPTION_PRICES['group'][group_size]['all'][period_type]
                        price = calculate_discounted_price(user_id, 'group', 'all', period_type, group_size)
                        text = f"📦 <b>Все режимы (на {group_size} участников)</b>\n\n"
                        text += "💎 <b>Что входит в подписку:</b>\n\n"
                        text += "🔔 <b>Уведомления о сериалах:</b>\n"
                        text += "• Автоматические уведомления о выходе новых серий\n"
                        text += "• Настройка времени уведомлений\n\n"
                        text += "🎯 <b>Персональные рекомендации:</b>\n"
                        text += "• Режим \"По оценкам в базе\" — рекомендации по оценкам фильмов, добавленных в базу чата или группы\n"
                        text += "• Режим \"Рандом по Кинопоиску\" — случайный фильм из Кинопоиска по фильтрам\n"
                        text += "• Режим рандомайзера \"По моим оценкам\" — рекомендации по оценкам из Кинопоиска\n"
                        text += "• Импорт базы из Кинопоиска\n\n"
                        text += "🎫 <b>Билеты в кино:</b>\n"
                        text += "• Добавление билетов на сеансы и мероприятия\n"
                        text += "• Настраиваемые уведомления с билетами перед мероприятием\n\n"
                        text += f"💰 <b>Стоимость:</b> {price}₽ за {period_name}"
                        if price < base_price:
                            text += f" <s>(было {base_price}₽)</s>"
            
                # Определяем формат периодичности для кнопки
                period_display = {
                    'month': '/мес',
                    '3months': ' за 3 мес',
                    'year': ' за год',
                    'lifetime': ' навсегда'
                }
                period_suffix = period_display.get(period_type, '')
            
                # Вычисляем финальную цену с учетом скидок
                if sub_type == 'personal':
                    final_price = calculate_discounted_price(user_id, 'personal', plan_type, period_type)
                else:  # group
                    final_price = calculate_discounted_price(user_id, 'group', plan_type, period_type, group_size)
            
                # Сохраняем состояние для подтверждения (используем final_price вместо price)
                if sub_type == 'personal':
                    if is_private:
                        telegram_username = call.from_user.username
                        user_payment_state[user_id] = {
                            'step': 'confirm_personal',
                            'subscription_type': sub_type,
                            'plan_type': plan_type,
                            'period_type': period_type,
                            'price': final_price,
                            'chat_id': chat_id,
                            'telegram_username': telegram_username
                        }
                    else:
                        user_payment_state[user_id] = {
                            'step': 'enter_personal_username',
                            'subscription_type': sub_type,
                            'plan_type': plan_type,
                            'period_type': period_type,
                            'price': final_price,
                            'chat_id': chat_id
                        }
                        text += "\n\nУкажите ваш ник в Telegram (можно с @ или без):"
                else:  # group
                    # Используем выбранную группу из состояния
                    state = user_payment_state.get(user_id, {})
                    if not group_chat_id:
                        group_chat_id = state.get('group_chat_id')
                        group_username = state.get('group_username')
                        group_title = state.get('group_title')
                
                    # Добавляем информацию о группе в описание, если она выбрана
                    if group_title:
                        group_info = f"👥 <b>Группа:</b> {group_title}\n"
                        if group_username:
                            group_info += f"@{group_username}\n\n"
                        text = group_info + text
                
                    if is_private:
                        # В личке - если группа не выбрана, просим выбрать
                        if not group_chat_id:
                            from moviebot.database.db_operations import get_user_groups
                            user_groups = get_user_groups(user_id, bot)
                            if not user_groups:
                                text = f"👥 <b>Групповые тарифы на {group_size} участников</b>\n\n"
                                text += "❌ Не найдено групп, где вы и бот состоите вместе.\n\n"
                                text += "Добавьте бота в группу и отправьте любое сообщение, чтобы группа появилась в списке."
                                markup = InlineKeyboardMarkup(row_width=1)
                                markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:group_size:{group_size}"))
                                try:
                                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                                except Exception as e:
                                    if "message is not modified" not in str(e):
                                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                                return
                        
                            # Показываем выбор группы
                            text = f"👥 <b>Выберите группу для подписки на {group_size} участников</b>\n\n"
                            text += "Выберите группу из списка:"
                        
                            markup = InlineKeyboardMarkup(row_width=1)
                            for group in user_groups[:10]:
                                group_title = group.get('title', f"Группа {group.get('chat_id')}")
                                group_username = group.get('username')
                                if group_username:
                                    button_text = f"📍 {group_title} (@{group_username})"
                                else:
                                    button_text = f"📍 {group_title}"
                                if len(button_text) > 50:
                                    button_text = button_text[:47] + "..."
                                markup.add(InlineKeyboardButton(
                                    button_text,
                                    callback_data=f"payment:select_group:{group_size}:{group.get('chat_id')}"
                                ))
                            markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:group_size:{group_size}"))
                        
                            try:
                                bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                            except Exception as e:
                                if "message is not modified" not in str(e):
                                    logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                            return
                    
                        # Группа выбрана - сохраняем состояние для подтверждения
                        user_payment_state[user_id] = {
                            'step': 'confirm_group',
                            'subscription_type': sub_type,
                            'plan_type': plan_type,
                            'period_type': period_type,
                            'price': final_price,
                            'group_size': group_size,
                            'chat_id': group_chat_id,  # Используем выбранную группу
                            'group_username': group_username,
                            'group_title': group_title
                        }
                    else:
                        # В группе - используем текущую группу
                        group_username = call.message.chat.username
                        group_chat_id = chat_id
                        # Проверяем количество активных пользователей
                        from moviebot.database.db_operations import get_active_group_users
                        # Вызываем функцию с одним аргументом для совместимости со старой версией
                        # bot_id не критичен - функция просто вернет всех активных пользователей, включая бота
                        active_users = get_active_group_users(chat_id, bot_id=BOT_ID)
                        active_count = len(active_users)
                    
                        if active_count > int(group_size):
                            # Нужно выбрать участников
                            text += f"\n\n⚠️ <b>Внимание!</b>\n"
                            text += f"В группе <b>{active_count}</b> активных участников, а вы выбираете подписку на <b>{group_size}</b>.\n"
                            text += "После подтверждения вы сможете выбрать участников для подписки."
                    
                        user_payment_state[user_id] = {
                            'step': 'confirm_group',
                            'subscription_type': sub_type,
                            'plan_type': plan_type,
                            'period_type': period_type,
                            'price': final_price,
                            'group_size': group_size,
                            'chat_id': chat_id,
                            'group_username': group_username
                        }
            
                # Определяем chat_id для платежа (до использования)
                if sub_type == 'group' and group_chat_id:
                    payment_chat_id = group_chat_id
                else:
                    payment_chat_id = chat_id
            
                # Показываем выбор способа оплаты
                # Конвертируем в звезды для кнопки оплаты звездами
                stars_amount = rubles_to_stars(final_price)
            
                # Сохраняем данные платежа в состояние для оплаты (чтобы не превышать лимит callback_data в 64 байта)
                import uuid as uuid_module
                payment_id = str(uuid_module.uuid4())
            
                if user_id not in user_payment_state:
                    user_payment_state[user_id] = {}
                user_payment_state[user_id]['payment_data'] = {
                    'payment_id': payment_id,
                    'sub_type': sub_type,
                    'group_size': group_size,
                    'plan_type': plan_type,
                    'period_type': period_type,
                    'amount': final_price,
                    'stars_amount': stars_amount,
                    'chat_id': payment_chat_id,
                    'group_chat_id': group_chat_id if sub_type == 'group' else None,
                    'group_username': group_username if sub_type == 'group' else None,
                    'group_title': group_title if sub_type == 'group' else None
                }
            
                # Проверяем, есть ли существующие подписки и нужно ли предлагать варианты переподписки
                need_resubscription_options = False
                existing_subs_for_resub = []
                total_existing_price = 0
                next_payment_date = None
                next_sub_for_resub = None
                
                if sub_type == 'personal' and not is_owner:
                    from moviebot.database.db_operations import get_user_personal_subscriptions
                    existing_subs_for_resub = get_user_personal_subscriptions(user_id)
                    
                    # Фильтруем только активные подписки
                    active_subs_for_resub = []
                    now = datetime.now(pytz.UTC)
                    for sub in existing_subs_for_resub:
                        expires_at = sub.get('expires_at')
                        is_active = False
                        if not expires_at:
                            is_active = True
                        elif isinstance(expires_at, datetime):
                            if expires_at.tzinfo is None:
                                expires_at = pytz.UTC.localize(expires_at)
                            if expires_at.tzinfo != pytz.UTC:
                                expires_at = expires_at.astimezone(pytz.UTC)
                            is_active = expires_at > now
                        else:
                            try:
                                if isinstance(expires_at, str):
                                    expires_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                                    if expires_dt.tzinfo is None:
                                        expires_dt = pytz.UTC.localize(expires_dt)
                                    if expires_dt.tzinfo != pytz.UTC:
                                        expires_dt = expires_dt.astimezone(pytz.UTC)
                                    is_active = expires_dt > now
                                else:
                                    is_active = True
                            except:
                                is_active = True
                        
                        if is_active:
                            active_subs_for_resub.append(sub)
                            total_existing_price += sub.get('price', 0)
                            
                            # Находим ближайшее следующее списание
                            sub_next_payment = sub.get('next_payment_date')
                            if sub_next_payment:
                                if not next_payment_date or (isinstance(sub_next_payment, datetime) and isinstance(next_payment_date, datetime) and sub_next_payment < next_payment_date):
                                    next_payment_date = sub_next_payment
                                    next_sub_for_resub = sub
                    
                    # Проверяем, нужно ли предлагать варианты переподписки
                    # Если есть существующие подписки и сумма новой подписки отличается от суммы существующих
                    if active_subs_for_resub and final_price != total_existing_price:
                        need_resubscription_options = True
                        existing_subs_for_resub = active_subs_for_resub
                
                # Если нужно предложить варианты переподписки
                if need_resubscription_options:
                    # Сохраняем информацию о существующих подписках в состоянии
                    if user_id not in user_payment_state:
                        user_payment_state[user_id] = {}
                    user_payment_state[user_id]['existing_subs'] = existing_subs_for_resub
                    user_payment_state[user_id]['next_sub'] = next_sub_for_resub
                    
                    plan_names = {
                        'notifications': 'Уведомления о сериалах',
                        'recommendations': 'Рекомендации',
                        'tickets': 'Билеты',
                        'all': 'Все режимы'
                    }
                    
                    text += f"\n\n⚠️ <b>У вас уже есть активные подписки:</b>\n"
                    for sub in existing_subs_for_resub:
                        plan_type_existing = sub.get('plan_type')
                        plan_name = plan_names.get(plan_type_existing, plan_type_existing)
                        text += f"• {plan_name}\n"
                    
                    text += f"\n💰 <b>Текущие подписки:</b> {total_existing_price}₽/мес\n"
                    text += f"💰 <b>Новая подписка:</b> {final_price}₽{period_suffix}\n\n"
                    
                    diff_price = final_price - total_existing_price
                    if diff_price > 0:
                        text += f"💡 <b>Доплата:</b> {diff_price}₽\n\n"
                    elif diff_price < 0:
                        text += f"💡 <b>Экономия:</b> {abs(diff_price)}₽\n\n"
                    
                    text += "Выберите способ оформления:\n\n"
                    text += "1️⃣ <b>Изменить сейчас</b> — текущие подписки будут отменены, новая подписка начнется после оплаты с датой списания в текущий день.\n\n"
                    
                    if next_payment_date and next_sub_for_resub and period_type == 'month':
                        text += f"2️⃣ <b>Увеличить со следующего списания</b> — текущие подписки будут отменены, сумма следующего списания будет изменена на {final_price}₽"
                        if isinstance(next_payment_date, datetime):
                            text += f" (дата: {next_payment_date.strftime('%d.%m.%Y')})"
                        text += "\n\n"
                    
                    # Добавляем информационное сообщение для всех тарифов, кроме "навсегда"
                    if period_type != 'lifetime':
                        text += "ℹ️ После оформления подписки, данные карты будут сохранены для проведения списаний по выбранному расписанию. В дальнейшем, подтверждать отдельно платежи не придется. Вы сможете отменить подписку в любой момент\n"
                    
                    markup = InlineKeyboardMarkup(row_width=1)
                    
                    # Кнопка "Изменить сейчас"
                    payment_id_short = payment_id[:8]
                    callback_data_stars = f"payment:pay_stars:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}:{payment_id_short}"
                    markup.add(InlineKeyboardButton(f"1️⃣ Изменить сейчас ({final_price}₽)", callback_data=callback_data_stars))
                    
                    # Кнопка "Увеличить со следующего списания" (только для месячных подписок и если есть следующее списание)
                    if next_payment_date and next_sub_for_resub and period_type == 'month':
                        markup.add(InlineKeyboardButton("2️⃣ Увеличить со следующего списания", callback_data=f"payment:combine:add_to_next:{plan_type}:{period_type}"))
                    
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:tariffs:personal"))
                else:
                    # Обычный поток - показываем стандартные кнопки оплаты
                    # Обновляем сообщение с кнопками выбора способа оплаты
                    text += f"\n\n💳 <b>Выберите способ оплаты</b>\n"
                    text += f"💰 Сумма: <b>{final_price}₽{period_suffix}</b> ({stars_amount}⭐)\n"
                    
                    # Добавляем информационное сообщение для всех тарифов, кроме "навсегда"
                    if period_type != 'lifetime':
                        text += "\nℹ️ После оформления подписки, данные карты будут сохранены для проведения списаний по выбранному расписанию. В дальнейшем, подтверждать отдельно платежи не придется. Вы сможете отменить подписку в любой момент\n"
                
                    markup = InlineKeyboardMarkup(row_width=1)
                    # Кнопка оплаты звездами (без ЮKassa)
                    payment_id_short = payment_id[:8]
                    # Используем формат payment:pay_stars:... для обработки в payment_callbacks.py
                    callback_data_stars = f"payment:pay_stars:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}:{payment_id_short}"
                    markup.add(InlineKeyboardButton(f"⭐ Оплатить звездами Telegram ({stars_amount}⭐)", callback_data=callback_data_stars))
                
                    # Кнопка оплаты через ЮKassa (только если доступна)
                    if YOOKASSA_AVAILABLE and YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY:
                        callback_data_yookassa = f"payment:pay_yookassa:{payment_id_short}"
                        markup.add(InlineKeyboardButton("💳 Оплатить картой/ЮMoney", callback_data=callback_data_yookassa))
                
                # Добавляем кнопку промокода
                # Сохраняем данные в состояние для использования короткого callback_data
                # user_id и chat_id уже определены выше
                user_promo_state[user_id] = {
                    'chat_id': payment_chat_id,  # Используем payment_chat_id для правильного chat_id
                    'message_id': call.message.message_id,
                    'sub_type': sub_type,
                    'plan_type': plan_type,
                    'period_type': period_type,
                    'group_size': group_size,
                    'payment_id': payment_id_short,
                    'original_price': final_price
                }
                # Используем короткий callback_data
                callback_data_promo = "payment:promo"
                markup.add(InlineKeyboardButton("🏷️ Промокод", callback_data=callback_data_promo))
            
                if group_size:
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:group_size:{group_size}"))
                else:
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:tariffs:{sub_type}"))
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                        bot_instance.send_message(call.message.chat.id, text, reply_markup=markup, parse_mode='HTML')
                return
        
            if action.startswith("pay:"):
                # Обработка нажатия на кнопку "Оплатить" - создание платежа через ЮKassa
                try:
                    bot_instance.answer_callback_query(call.id)
                except:
                    pass
            
                # Проверяем, есть ли состояние для платежа
                state = user_payment_state.get(user_id, {})
                if state.get('step') == 'pay':
                    # Используем данные из состояния
                    sub_type = state.get('subscription_type', 'personal')
                    plan_type = state.get('plan_type', '')
                    period_type = state.get('period_type', '')
                    final_price = state.get('price', 0)
                    group_size = state.get('group_size')
                    is_combined = state.get('is_combined', False)
                else:
                    # Парсим из callback_data
                    parts = action.split(":")
                    sub_type = parts[1]  # personal или group
                
                    # Правильный парсинг: payment:pay:personal::tickets:month или payment:pay:group:2:all:month
                    if len(parts) >= 5:
                        # Есть group_size (для групп)
                        group_size_str = parts[2] if parts[2] else ''
                        group_size = int(group_size_str) if group_size_str and group_size_str.isdigit() else None
                        plan_type = parts[3] if parts[3] else ''
                        period_type = parts[4] if parts[4] else ''
                    else:
                        # Нет group_size (для личных)
                        group_size = None
                        plan_type = parts[2] if len(parts) > 2 and parts[2] else ''
                    period_type = parts[3] if len(parts) > 3 and parts[3] else ''
            
                # Проверка на пустые значения
                if not plan_type or not period_type:
                    logger.error(f"[PAYMENT] Ошибка парсинга callback_data: {action}, parts={parts}")
                    bot_instance.answer_callback_query(call.id, "Ошибка: неверные параметры платежа", show_alert=True)
                    return
            
                # Вычисляем финальную цену с учетом скидок
                if sub_type == 'personal':
                    final_price = calculate_discounted_price(user_id, 'personal', plan_type, period_type)
                else:  # group
                    final_price = calculate_discounted_price(user_id, 'group', plan_type, period_type, group_size)
                
                # Проверяем, есть ли примененный промокод в состоянии
                payment_state = user_payment_state.get(user_id, {})
                if payment_state.get('promocode_id') and payment_state.get('price'):
                    # Используем цену с промокодом
                    final_price = payment_state['price']
                    logger.info(f"[PAYMENT] Используется промокод: {payment_state.get('promocode')}, цена: {final_price}₽")
            
                if final_price <= 0:
                    bot_instance.answer_callback_query(call.id, "Ошибка: неверная сумма платежа", show_alert=True)
                    return
                
                    is_combined = False
            
                # Обновляем состояние для передачи в metadata
                if state.get('step') != 'pay':
                    state = user_payment_state.get(user_id, {})
            
                logger.info(f"[PAYMENT] Расчет цены: user_id={user_id}, sub_type={sub_type}, plan_type={plan_type}, period_type={period_type}, final_price={final_price}₽")
            
                # Инициализируем ЮKassa
                if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
                    logger.error(f"[PAYMENT] YooKassa ключи не настроены! YOOKASSA_SHOP_ID={YOOKASSA_SHOP_ID is not None}, YOOKASSA_SECRET_KEY={YOOKASSA_SECRET_KEY is not None}")
                    bot_instance.answer_callback_query(call.id, "Ошибка: ключи оплаты не настроены. Обратитесь к администратору.", show_alert=True)
                    return
            
                # Логируем первые и последние символы для отладки (безопасно)
                shop_id_preview = f"{YOOKASSA_SHOP_ID[:4]}...{YOOKASSA_SHOP_ID[-4:]}" if YOOKASSA_SHOP_ID and len(YOOKASSA_SHOP_ID) > 8 else "N/A"
                secret_key_preview = f"{YOOKASSA_SECRET_KEY[:4]}...{YOOKASSA_SECRET_KEY[-4:]}" if YOOKASSA_SECRET_KEY and len(YOOKASSA_SECRET_KEY) > 8 else "N/A"
                logger.info(f"[PAYMENT] Инициализация YooKassa: shop_id={shop_id_preview}, secret_key={secret_key_preview}")
            
                # Убираем пробелы, если есть
                shop_id = YOOKASSA_SHOP_ID.strip() if YOOKASSA_SHOP_ID else None
                secret_key = YOOKASSA_SECRET_KEY.strip() if YOOKASSA_SECRET_KEY else None
            
                from yookassa import Configuration, Payment
                Configuration.account_id = shop_id
                Configuration.secret_key = secret_key
            
                # Формируем описание платежа
                period_names = {
                    'month': 'месяц',
                    '3months': '3 месяца',
                    'year': 'год',
                    'lifetime': 'навсегда'
                }
                period_name = period_names.get(period_type, period_type)
            
                plan_names = {
                    'notifications': 'Уведомления о сериалах',
                    'recommendations': 'Персональные рекомендации',
                    'tickets': 'Билеты в кино',
                    'all': 'Все режимы'
                }
                plan_name = plan_names.get(plan_type, plan_type)
            
                subscription_type_name = 'Личная подписка' if sub_type == 'personal' else f'Групповая подписка (на {group_size} участников)'
                description = f"{subscription_type_name}: {plan_name}, период: {period_name}"
            
                # Создаем уникальный ID платежа
                import uuid as uuid_module
                payment_id = str(uuid_module.uuid4())
            
                # Определяем URL для возврата (нужно будет настроить в зависимости от вашего домена)
                # Определяем URL для возврата - используем deep link для Telegram
                return_url = os.getenv('YOOKASSA_RETURN_URL', 'tg://resolve?domain=movie_planner_bot')
            
                # Подготавливаем metadata для платежа
                metadata = {
                    "user_id": str(user_id),
                    "chat_id": str(chat_id),
                    "subscription_type": sub_type,
                    "plan_type": plan_type,
                    "period_type": period_type,
                    "payment_id": payment_id
                }
            
                # Проверяем, является ли это объединенным платежом или расширением
                payment_state = user_payment_state.get(user_id, {})
                is_combined = payment_state.get('is_combined', False)
                is_expansion = payment_state.get('is_expansion', False)
                
                if is_combined:
                    combine_type = payment_state.get('combine_type')
                    existing_subs = payment_state.get('existing_subs', [])
                    metadata["is_combined"] = "true"
                    metadata["combine_type"] = combine_type
                    if existing_subs:
                        existing_subs_ids = [str(sub.get('id')) for sub in existing_subs if sub.get('id')]
                        metadata["existing_subs_ids"] = ','.join(existing_subs_ids)
                        logger.info(f"[PAYMENT] Объединенный платеж: combine_type={combine_type}, existing_subs_ids={metadata['existing_subs_ids']}")
                
                if is_expansion:
                    expansion_subscription_id = payment_state.get('expansion_subscription_id')
                    expansion_current_size = payment_state.get('expansion_current_size')
                    expansion_new_size = payment_state.get('expansion_new_size')
                    metadata["is_expansion"] = "true"
                    metadata["expansion_subscription_id"] = str(expansion_subscription_id) if expansion_subscription_id else ""
                    metadata["expansion_current_size"] = str(expansion_current_size) if expansion_current_size else ""
                    metadata["expansion_new_size"] = str(expansion_new_size) if expansion_new_size else ""
                    logger.info(f"[PAYMENT] Расширение подписки: subscription_id={expansion_subscription_id}, {expansion_current_size}->{expansion_new_size}")
            
                # Добавляем group_size, telegram_username или group_username в зависимости от типа подписки
                if sub_type == 'group':
                    metadata["group_size"] = str(group_size) if group_size else ""
                    if not is_private:
                        # В группе - сохраняем username группы
                        group_username = call.message.chat.username
                        if group_username:
                            metadata["group_username"] = group_username
                else:
                    # Для личной подписки
                    if is_private:
                        # В личке - сохраняем username пользователя
                        telegram_username = call.from_user.username
                        if telegram_username:
                            metadata["telegram_username"] = telegram_username
            
                # Создаем платеж
                # Для всех подписок кроме lifetime добавляем save_payment_method: True
                payment_data = {
                    "amount": {
                        "value": f"{final_price:.2f}",
                        "currency": "RUB"
                    },
                    "confirmation": {
                        "type": "redirect",
                        "return_url": return_url
                    },
                    "capture": True,
                    "description": description,
                    "metadata": metadata
                }
                
                # Добавляем save_payment_method для всех не-lifetime подписок
                if period_type != 'lifetime':
                    payment_data["save_payment_method"] = True
                    logger.info(f"[YOOKASSA] save_payment_method=True добавлен для period_type={period_type}")
                
                try:
                    payment = Payment.create(payment_data)
                
                    # Сохраняем информацию о платеже в БД
                    from moviebot.database.db_operations import save_payment
                    # Для групповых подписок используем выбранный chat_id группы
                    payment_chat_id_for_db = payment_chat_id if sub_type == 'group' and group_chat_id else chat_id
                    save_payment(
                        payment_id=payment_id,
                        yookassa_payment_id=payment.id,
                        user_id=user_id,
                        chat_id=payment_chat_id_for_db,
                        subscription_type=sub_type,
                        plan_type=plan_type,
                        period_type=period_type,
                        group_size=group_size,
                        amount=final_price,
                        status='pending'
                    )
                
                    # Получаем URL для оплаты
                    confirmation_url = payment.confirmation.confirmation_url
                
                    # Отправляем сообщение с кнопкой для оплаты
                    text = f"💳 <b>Оплата подписки</b>\n\n"
                    text += f"📋 <b>Выбранный тариф:</b>\n"
                    if sub_type == 'personal':
                        text += f"👤 Личная подписка\n"
                    else:
                        text += f"👥 Групповая подписка (на {group_size} участников)\n"
                    text += f"{plan_names.get(plan_type, plan_type)}\n"
                    text += f"⏰ Период: {period_name}\n"
                    text += f"💰 Сумма: <b>{final_price}₽</b>\n\n"
                    
                    # Добавляем информационное сообщение для всех тарифов, кроме "навсегда"
                    if period_type != 'lifetime':
                        text += "ℹ️ После оформления подписки, данные карты будут сохранены для проведения списаний по выбранному расписанию. В дальнейшем, подтверждать отдельно платежи не придется. Вы сможете отменить подписку в любой момент\n\n"
                    
                    text += "Нажмите кнопку ниже для перехода к оплате:"
                
                    # Конвертируем в звезды для кнопки оплаты звездами
                    stars_amount = rubles_to_stars(final_price)
                
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("💳 Оплатить", url=confirmation_url))
                    # Добавляем кнопку оплаты звездами
                    callback_data_stars = f"payment:pay_stars:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}:{payment_id}"
                    markup.add(InlineKeyboardButton(f"⭐ Оплатить звездами Telegram ({stars_amount}⭐)", callback_data=callback_data_stars))
                    # Добавляем кнопку промокода
                    # Сохраняем данные в состояние для использования короткого callback_data
                    user_id = call.from_user.id
                    user_promo_state[user_id] = {
                        'chat_id': chat_id,
                        'message_id': call.message.message_id,
                        'sub_type': sub_type,
                        'plan_type': plan_type,
                        'period_type': period_type,
                        'group_size': group_size,
                        'payment_id': payment_id,
                        'original_price': final_price
                    }
                    # Используем короткий callback_data
                    callback_data_promo = "payment:promo"
                    markup.add(InlineKeyboardButton("🏷️ Промокод", callback_data=callback_data_promo))
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:subscribe:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}" if group_size else f"payment:subscribe:{sub_type}:{plan_type}:{period_type}"))
                
                    try:
                        bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    except Exception as e:
                        if "message is not modified" not in str(e):
                            logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                            bot_instance.send_message(call.message.chat.id, text, reply_markup=markup, parse_mode='HTML')
                
                except Exception as e:
                    logger.error(f"[PAYMENT] Ошибка создания платежа в ЮKassa: {e}", exc_info=True)
                    bot_instance.answer_callback_query(call.id, "Ошибка создания платежа. Попробуйте позже.", show_alert=True)
                return
        
            if action.startswith("pay_stars:"):
                # Обработка нажатия на кнопку "Оплатить звездами Telegram"
                try:
                    bot_instance.answer_callback_query(call.id)
                except:
                    pass
            
                # Пытаемся получить данные из состояния (новый формат)
                state = user_payment_state.get(user_id, {})
                payment_data = state.get('payment_data', {})
                
                if payment_data:
                    # Используем данные из состояния (новый формат)
                    sub_type = payment_data.get('sub_type', 'personal')
                    plan_type = payment_data.get('plan_type', '')
                    period_type = payment_data.get('period_type', '')
                    final_price = payment_data.get('amount', 0)
                    group_size = payment_data.get('group_size')
                    payment_id = payment_data.get('payment_id', '')
                    payment_chat_id = payment_data.get('chat_id', chat_id)
                    group_chat_id = payment_data.get('group_chat_id')
                    
                    # Проверяем, есть ли примененный промокод в состоянии (может быть обновлен после создания payment_data)
                    payment_state = user_payment_state.get(user_id, {})
                    if payment_state.get('promocode_id') and payment_state.get('price'):
                        # Используем цену с промокодом из состояния (приоритет над payment_data)
                        final_price = payment_state['price']
                        logger.info(f"[STARS] Используется промокод из состояния: {payment_state.get('promocode')}, цена: {final_price}₽")
                else:
                    # Старый формат: парсим из callback_data
                    parts = action.split(":")
                    # Формат: payment:pay_stars:personal::tickets:month:payment_id
                    # или: payment:pay_stars:group:2:all:month:payment_id
                    if len(parts) < 6:
                        logger.error(f"[STARS] Ошибка парсинга callback_data: {action}, parts={parts}")
                        bot_instance.answer_callback_query(call.id, "Ошибка: неверные параметры платежа", show_alert=True)
                        return
                    
                    sub_type = parts[1]  # personal или group
                    group_size_str = parts[2] if parts[2] else ''
                    group_size = int(group_size_str) if group_size_str and group_size_str.isdigit() else None
                    plan_type = parts[3] if parts[3] else ''
                    period_type = parts[4] if parts[4] else ''
                    payment_id = parts[5] if len(parts) > 5 else ''
                    payment_chat_id = chat_id
                    group_chat_id = None
                    
                    # Вычисляем финальную цену с учетом скидок
                    if sub_type == 'personal':
                        final_price = calculate_discounted_price(user_id, 'personal', plan_type, period_type)
                    else:  # group
                        final_price = calculate_discounted_price(user_id, 'group', plan_type, period_type, group_size)
                    
                    # Проверяем, есть ли примененный промокод в состоянии
                    payment_state = user_payment_state.get(user_id, {})
                    if payment_state.get('promocode_id') and payment_state.get('price'):
                        # Используем цену с промокодом
                        final_price = payment_state['price']
                        logger.info(f"[STARS] Используется промокод: {payment_state.get('promocode')}, цена: {final_price}₽")
            
                # Проверка на пустые значения
                if not plan_type or not period_type:
                    logger.error(f"[STARS] Ошибка: неверные параметры платежа: plan_type={plan_type}, period_type={period_type}")
                    bot_instance.answer_callback_query(call.id, "Ошибка: неверные параметры платежа", show_alert=True)
                    return
            
                if final_price <= 0:
                    bot_instance.answer_callback_query(call.id, "Ошибка: неверная сумма платежа", show_alert=True)
                    return
            
                # Конвертируем рубли в звезды
                stars_amount = rubles_to_stars(final_price)
            
                logger.info(f"[STARS] Расчет звезд: user_id={user_id}, sub_type={sub_type}, plan_type={plan_type}, period_type={period_type}, final_price={final_price}₽, stars_amount={stars_amount}⭐")
            
                # Формируем описание платежа
                period_names = {
                    'month': 'месяц',
                    '3months': '3 месяца',
                    'year': 'год',
                    'lifetime': 'навсегда'
                }
                period_name = period_names.get(period_type, period_type)
            
                plan_names = {
                    'notifications': 'Уведомления о сериалах',
                    'recommendations': 'Персональные рекомендации',
                    'tickets': 'Билеты в кино',
                    'all': 'Все режимы'
                }
                plan_name = plan_names.get(plan_type, plan_type)
            
                subscription_type_name = 'Личная подписка' if sub_type == 'personal' else f'Групповая подписка (на {group_size} участников)'
                title = f"{subscription_type_name}: {plan_name}"
                description = f"Период: {period_name}\nСумма: {final_price}₽ ({stars_amount}⭐)"
            
                # Создаем уникальный payload для платежа
                if not payment_id:
                    import uuid as uuid_module
                    payment_id = str(uuid_module.uuid4())
            
                # payment_chat_id уже установлен выше из payment_data или chat_id
            
                # Сохраняем информацию о платеже в БД
                from moviebot.database.db_operations import save_payment
                save_payment(
                    payment_id=payment_id,
                    yookassa_payment_id=None,  # Для Stars нет yookassa_payment_id
                    user_id=user_id,
                    chat_id=payment_chat_id,
                    subscription_type=sub_type,
                    plan_type=plan_type,
                    period_type=period_type,
                    group_size=group_size,
                    amount=final_price,
                    status='pending'
                )
            
                # Создаем payload для инвойса (должен быть уникальным)
                invoice_payload = f"stars_{payment_id}"
            
                # Определяем subscription_period для подписок (кроме lifetime)
                # Согласно документации: https://core.telegram.org/api/subscriptions#bot-subscriptions
                # subscription_period определяет интервал автоматического списания
                subscription_period = None
                if period_type == 'month':
                    # Месячная подписка: списание каждые 30 дней
                    subscription_period = 30 * 24 * 60 * 60  # 30 дней в секундах
                elif period_type == '3months':
                    # Подписка на 3 месяца: списание каждые 90 дней
                    subscription_period = 90 * 24 * 60 * 60  # 90 дней в секундах
                elif period_type == 'year':
                    # Годовая подписка: списание каждые 365 дней
                    subscription_period = 365 * 24 * 60 * 60  # 365 дней в секундах
                elif period_type == 'test':
                    # Тестовая подписка: списание каждые 10 минут
                    subscription_period = 10 * 60  # 10 минут в секундах
                # Для lifetime не создаем подписку (subscription_period = None)
                
                if subscription_period:
                    logger.info(f"[STARS] Создается подписка с периодом {subscription_period} секунд ({period_type})")
            
                # Отправляем инвойс через Telegram Stars
                try:
                    success = create_stars_invoice(
                        bot=bot,
                        chat_id=call.message.chat.id,
                        title=title,
                        description=description,
                        payload=invoice_payload,
                        stars_amount=stars_amount,
                        subscription_period=subscription_period
                    )
                
                    if success:
                        logger.info(f"[STARS] Инвойс отправлен: user_id={user_id}, payment_id={payment_id}, stars={stars_amount}, price={final_price}₽")
                    else:
                        bot_instance.answer_callback_query(call.id, "Ошибка создания инвойса. Попробуйте позже.", show_alert=True)
                except Exception as e:
                    logger.error(f"[STARS] Ошибка создания инвойса через Stars: {e}", exc_info=True)
                    bot_instance.answer_callback_query(call.id, "Ошибка создания инвойса. Попробуйте позже.", show_alert=True)
                return
        
            if action.startswith("pay_yookassa:"):
                # Обработка нажатия на кнопку "Оплатить картой/ЮMoney" через YooKassa
                try:
                    bot_instance.answer_callback_query(call.id)
                except:
                    pass
                
                # Получаем payment_id из callback_data
                parts = action.split(":")
                payment_id_short = parts[1] if len(parts) > 1 else ''
                
                # Получаем данные платежа из состояния
                state = user_payment_state.get(user_id, {})
                payment_data = state.get('payment_data', {})
                
                if not payment_data:
                    logger.error(f"[YOOKASSA] Не найдены данные платежа в состоянии для user_id={user_id}")
                    bot_instance.answer_callback_query(call.id, "Ошибка: данные платежа не найдены. Начните заново.", show_alert=True)
                    return
                
                sub_type = payment_data.get('sub_type', 'personal')
                plan_type = payment_data.get('plan_type', '')
                period_type = payment_data.get('period_type', '')
                final_price = payment_data.get('amount', 0)
                group_size = payment_data.get('group_size')
                payment_chat_id = payment_data.get('chat_id', chat_id)
                group_chat_id = payment_data.get('group_chat_id')
                group_username = payment_data.get('group_username')
                group_title = payment_data.get('group_title')
                
                if not plan_type or not period_type or final_price <= 0:
                    logger.error(f"[YOOKASSA] Неверные данные платежа: plan_type={plan_type}, period_type={period_type}, final_price={final_price}")
                    bot_instance.answer_callback_query(call.id, "Ошибка: неверные параметры платежа", show_alert=True)
                    return
                
                logger.info(f"[YOOKASSA] Создание платежа: user_id={user_id}, sub_type={sub_type}, plan_type={plan_type}, period_type={period_type}, final_price={final_price}₽")
                
                # Инициализируем ЮKassa
                if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY:
                    logger.error(f"[YOOKASSA] YooKassa ключи не настроены!")
                    bot_instance.answer_callback_query(call.id, "Ошибка: ключи оплаты не настроены. Обратитесь к администратору.", show_alert=True)
                    return
                
                shop_id = YOOKASSA_SHOP_ID.strip() if YOOKASSA_SHOP_ID else None
                secret_key = YOOKASSA_SECRET_KEY.strip() if YOOKASSA_SECRET_KEY else None
                
                from yookassa import Configuration, Payment
                Configuration.account_id = shop_id
                Configuration.secret_key = secret_key
                
                # Формируем описание платежа
                period_names = {
                    'month': 'месяц',
                    '3months': '3 месяца',
                    'year': 'год',
                    'lifetime': 'навсегда'
                }
                period_name = period_names.get(period_type, period_type)
                
                plan_names = {
                    'notifications': 'Уведомления о сериалах',
                    'recommendations': 'Персональные рекомендации',
                    'tickets': 'Билеты в кино',
                    'all': 'Все режимы'
                }
                plan_name = plan_names.get(plan_type, plan_type)
                
                subscription_type_name = 'Личная подписка' if sub_type == 'personal' else f'Групповая подписка (на {group_size} участников)'
                description = f"{subscription_type_name}: {plan_name}, период: {period_name}"
                
                # Создаем уникальный ID платежа
                import uuid as uuid_module
                payment_id = str(uuid_module.uuid4())
                
                return_url = os.getenv('YOOKASSA_RETURN_URL', 'tg://resolve?domain=movie_planner_bot')
                
                # Подготавливаем metadata для платежа
                metadata = {
                    "user_id": str(user_id),
                    "chat_id": str(payment_chat_id),
                    "subscription_type": sub_type,
                    "plan_type": plan_type,
                    "period_type": period_type,
                    "payment_id": payment_id
                }
                
                if sub_type == 'group':
                    metadata["group_size"] = str(group_size) if group_size else ""
                    if group_username:
                        metadata["group_username"] = group_username
                
                # Создаем платеж
                # Для всех подписок кроме lifetime добавляем save_payment_method: True
                payment_data = {
                    "amount": {
                        "value": f"{final_price:.2f}",
                        "currency": "RUB"
                    },
                    "confirmation": {
                        "type": "redirect",
                        "return_url": return_url
                    },
                    "capture": True,
                    "description": description,
                    "metadata": metadata
                }
                
                # Добавляем save_payment_method для всех не-lifetime подписок
                if period_type != 'lifetime':
                    payment_data["save_payment_method"] = True
                    logger.info(f"[YOOKASSA] save_payment_method=True добавлен для period_type={period_type}")
                
                try:
                    payment = Payment.create(payment_data)
                    
                    # Сохраняем информацию о платеже в БД
                    from moviebot.database.db_operations import save_payment
                    save_payment(
                        payment_id=payment_id,
                        yookassa_payment_id=payment.id,
                        user_id=user_id,
                        chat_id=payment_chat_id,
                        subscription_type=sub_type,
                        plan_type=plan_type,
                        period_type=period_type,
                        group_size=group_size,
                        amount=final_price,
                        status='pending'
                    )
                    
                    # Получаем URL для оплаты
                    confirmation_url = payment.confirmation.confirmation_url
                    
                    # Отправляем сообщение с кнопкой для оплаты
                    text = f"💳 <b>Оплата подписки</b>\n\n"
                    text += f"📋 <b>Выбранный тариф:</b>\n"
                    if sub_type == 'personal':
                        text += f"👤 Личная подписка\n"
                    else:
                        text += f"👥 Групповая подписка (на {group_size} участников)\n"
                    text += f"{plan_name}\n"
                    text += f"⏰ Период: {period_name}\n"
                    text += f"💰 Сумма: <b>{final_price}₽</b>\n\n"
                    
                    # Добавляем информационное сообщение для всех тарифов, кроме "навсегда"
                    if period_type != 'lifetime':
                        text += "ℹ️ После оформления подписки, данные карты будут сохранены для проведения списаний по выбранному расписанию. В дальнейшем, подтверждать отдельно платежи не придется. Вы сможете отменить подписку в любой момент\n\n"
                    
                    text += "Нажмите кнопку ниже для перехода к оплате:"
                    
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("💳 Оплатить", url=confirmation_url))
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:subscribe:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}" if group_size else f"payment:subscribe:{sub_type}:{plan_type}:{period_type}"))
                    
                    try:
                        bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    except Exception as e:
                        if "message is not modified" not in str(e):
                            logger.error(f"[YOOKASSA] Ошибка редактирования сообщения: {e}")
                            bot_instance.send_message(call.message.chat.id, text, reply_markup=markup, parse_mode='HTML')
                    
                    logger.info(f"[YOOKASSA] Платеж создан: payment_id={payment_id}, yookassa_id={payment.id}, url={confirmation_url}")
                    
                except Exception as e:
                    logger.error(f"[YOOKASSA] Ошибка создания платежа: {e}", exc_info=True)
                    bot_instance.answer_callback_query(call.id, "Ошибка создания платежа. Попробуйте позже.", show_alert=True)
                return
            
                # Определяем доступные варианты продления
                available_periods = []
                if period_type == 'month':
                    available_periods = ['3months', 'year', 'lifetime']
                elif period_type == '3months':
                    available_periods = ['year', 'lifetime']
                elif period_type == 'year':
                    available_periods = ['lifetime']
            
                # Формируем текст и кнопки
                plan_names = {
                    'notifications': '🔔 Уведомления о сериалах',
                    'recommendations': '🎯 Персональные рекомендации',
                    'tickets': '🎫 Билеты в кино',
                    'all': '📦 Все режимы'
                }
            
                period_names = {
                    'month': 'месяц',
                    '3months': '3 месяца',
                    'year': 'год',
                    'lifetime': 'навсегда'
                }
            
                text = f"✏️ <b>Изменение подписки</b>\n\n"
                text += f"📋 <b>Текущая подписка:</b>\n"
                if subscription_type == 'personal':
                    text += f"👤 Личная подписка\n"
                else:
                    text += f"👥 Групповая подписка\n"
                    # Добавляем информацию о группе для групповой подписки
                    if chat_id_sub:
                        try:
                            chat = bot_instance.get_chat(chat_id_sub)
                            group_title = chat.title
                            group_username = chat.username
                            text += f"Группа: <b>{group_title}</b>\n"
                            if group_username:
                                text += f"@{group_username}\n"
                        except Exception as chat_error:
                            logger.error(f"[PAYMENT] Ошибка получения информации о группе: {chat_error}")
                text += f"{plan_names.get(plan_type, plan_type)}\n"
                text += f"⏰ Период: {period_names.get(period_type, period_type)}\n\n"
            
                markup = InlineKeyboardMarkup(row_width=1)
            
                # Для групповых подписок с отдельными функциями (notifications, recommendations, tickets)
                # показываем другие подписки и пакетную на месяц, а не варианты продления
                if subscription_type == 'group' and plan_type in ['notifications', 'recommendations', 'tickets']:
                    text += "💡 <b>Доступные подписки:</b>\n\n"
                    group_size_str = str(group_size) if group_size else '2'
                
                    # Показываем другие отдельные подписки
                    other_plans = []
                    if plan_type == 'notifications':
                        other_plans = ['recommendations', 'tickets']
                    elif plan_type == 'recommendations':
                        other_plans = ['notifications', 'tickets']
                    elif plan_type == 'tickets':
                        other_plans = ['notifications', 'recommendations']
                
                    for other_plan in other_plans:
                        other_price = SUBSCRIPTION_PRICES['group'][group_size_str][other_plan].get('month', 0)
                        if other_price > 0:
                            markup.add(InlineKeyboardButton(
                                f"{plan_names.get(other_plan, other_plan)} ({other_price}₽/мес)",
                                callback_data=f"payment:upgrade_plan:{subscription_id}:{other_plan}"
                            ))
                
                    # Показываем пакетную подписку на месяц
                    all_price = SUBSCRIPTION_PRICES['group'][group_size_str]['all'].get('month', 0)
                    if all_price > 0:
                        markup.add(InlineKeyboardButton(
                            f"{plan_names.get('all', 'all')} ({all_price}₽/мес)",
                            callback_data=f"payment:upgrade_plan:{subscription_id}:all"
                        ))
                else:
                    # Для других случаев показываем варианты продления периода
                    if available_periods:
                        text += "📅 <b>Продлить подписку:</b>\n"
                        for period in available_periods:
                            if subscription_type == 'personal':
                                price = SUBSCRIPTION_PRICES['personal'][plan_type].get(period, 0)
                            else:
                                group_size_str = str(group_size) if group_size else '2'
                                price = SUBSCRIPTION_PRICES['group'][group_size_str][plan_type].get(period, 0)
                        
                            # Показываем только варианты с ценой больше 0
                            if price > 0:
                                period_name = period_names.get(period, period)
                                if period == '3months':
                                    price_text = f"{price}₽/3 мес"
                                elif period == 'year':
                                    price_text = f"{price}₽/год"
                                elif period == 'lifetime':
                                    price_text = f"{price}₽"
                                else:
                                    price_text = f"{price}₽/мес"
                            
                                if subscription_type == 'personal':
                                    markup.add(InlineKeyboardButton(f"📅 {period_name.capitalize()} ({price_text})", callback_data=f"payment:subscribe:personal:{plan_type}:{period}"))
                                else:
                                    markup.add(InlineKeyboardButton(f"📅 {period_name.capitalize()} ({price_text})", callback_data=f"payment:subscribe:group:{group_size}:{plan_type}:{period}:{chat_id_sub}"))
            
                # Для групповых подписок - проверяем возможность расширения
                if subscription_type == 'group' and plan_type == 'all':
                    from moviebot.database.db_operations import get_active_group_users
                    try:
                        active_users = get_active_group_users(chat_id_sub, bot_id=BOT_ID)
                        active_count = len(active_users) if active_users else 0
                        current_size = group_size or 2
                    
                        # Предлагаем расширение, если в группе достаточно участников (минус бот)
                        if active_count - 1 > current_size:
                            if current_size == 2:
                                # Можно расширить до 5 или 10
                                if active_count - 1 >= 5:
                                    text += "\n👥 <b>Расширить подписку:</b>\n"
                                    for new_size in [5, 10]:
                                        if active_count - 1 >= new_size:
                                            current_price = SUBSCRIPTION_PRICES['group']['2'][plan_type].get(period_type, 0)
                                            new_price = SUBSCRIPTION_PRICES['group'][str(new_size)][plan_type].get(period_type, 0)
                                            diff = new_price - current_price
                                        
                                            # Применяем скидку, если есть личная подписка
                                            from moviebot.database.db_operations import get_user_personal_subscriptions
                                            personal_subs = get_user_personal_subscriptions(user_id)
                                            if personal_subs:
                                                if new_size == 5:
                                                    diff = int(diff * 0.5)
                                                elif new_size == 10:
                                                    diff = int(new_price * 0.5) - current_price
                                        
                                            markup.add(InlineKeyboardButton(f"👥 До {new_size} участников (+{diff}₽)", callback_data=f"payment:expand:{new_size}:{subscription_id}"))
                            elif current_size == 5:
                                # Можно расширить до 10
                                if active_count - 1 >= 10:
                                    text += "\n👥 <b>Расширить подписку:</b>\n"
                                    current_price = SUBSCRIPTION_PRICES['group']['5'][plan_type].get(period_type, 0)
                                    new_price = SUBSCRIPTION_PRICES['group']['10'][plan_type].get(period_type, 0)
                                    diff = new_price - current_price
                                
                                    # Применяем скидку, если есть личная подписка
                                    from moviebot.database.db_operations import get_user_personal_subscriptions
                                    personal_subs = get_user_personal_subscriptions(user_id)
                                    if personal_subs:
                                        diff = int(new_price * 0.5) - current_price
                                
                                    markup.add(InlineKeyboardButton(f"👥 До 10 участников (+{diff}₽)", callback_data=f"payment:expand:10:{subscription_id}"))
                    except Exception as e:
                        logger.error(f"[PAYMENT] Ошибка проверки активных пользователей: {e}", exc_info=True)
            
                # Кнопка "Назад"
                if subscription_type == 'personal':
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:personal"))
                else:
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group:current"))
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action == "cancel":
                # Отмена подписки
                personal_sub = get_active_subscription(chat_id, user_id, 'personal')
                group_sub = get_active_subscription(chat_id, user_id, 'group')
            
                markup = InlineKeyboardMarkup(row_width=1)
                if personal_sub:
                    markup.add(InlineKeyboardButton("❌ Отменить личную подписку", callback_data="payment:cancel:personal"))
                if group_sub:
                    markup.add(InlineKeyboardButton("❌ Отменить групповую подписку", callback_data="payment:cancel:group"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:back"))
            
                try:
                    bot_instance.edit_message_text(
                        "❌ <b>Отмена подписки</b>\n\nВыберите подписку для отмены:",
                        call.message.chat.id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action.startswith("upgrade_plan:"):
                # Обновление подписки до другого типа (например, с "notifications" до "all")
                try:
                    bot_instance.answer_callback_query(call.id)
                except:
                    pass
            
                # Парсим callback_data: payment:upgrade_plan:{subscription_id}:{new_plan_type}
                parts = action.split(":")
                if len(parts) < 3:
                    bot_instance.answer_callback_query(call.id, "Ошибка: неверный формат", show_alert=True)
                    return
            
                subscription_id = int(parts[1])
                new_plan_type = parts[2]
            
                # Получаем информацию о текущей подписке
                from moviebot.database.db_operations import get_subscription_by_id
                sub = get_subscription_by_id(subscription_id)
            
                if not sub or sub.get('user_id') != user_id:
                    bot_instance.answer_callback_query(call.id, "Подписка не найдена", show_alert=True)
                    return
            
                current_plan_type = sub.get('plan_type')
                period_type = sub.get('period_type', 'month')
                group_size = sub.get('group_size')
                subscription_type = sub.get('subscription_type')
            
                if subscription_type != 'group':
                    bot_instance.answer_callback_query(call.id, "Эта функция доступна только для групповых подписок", show_alert=True)
                    return
            
                if current_plan_type == new_plan_type:
                    bot_instance.answer_callback_query(call.id, "У вас уже есть эта подписка", show_alert=True)
                    return
            
                # Вычисляем цену для новой подписки
                group_size_str = str(group_size) if group_size else '2'
            
                # Для отдельных функций (notifications, recommendations, tickets) доступна только месячная подписка
                # Для "all" используем текущий период подписки
                if new_plan_type in ['notifications', 'recommendations', 'tickets']:
                    # Отдельные функции - только месячная подписка
                    new_price = SUBSCRIPTION_PRICES['group'][group_size_str][new_plan_type].get('month', 0)
                    current_month_price = SUBSCRIPTION_PRICES['group'][group_size_str][current_plan_type].get('month', 0)
                    upgrade_price = new_price - current_month_price
                else:
                    # Для "all" используем текущий период
                    new_price = SUBSCRIPTION_PRICES['group'][group_size_str][new_plan_type].get(period_type, 0)
                    current_price = SUBSCRIPTION_PRICES['group'][group_size_str][current_plan_type].get(period_type, 0)
                    upgrade_price = new_price - current_price
            
                # Получаем информацию о следующем списании
                next_payment_date = sub.get('next_payment_date')
                current_price = sub.get('price', 0)
                
                # Формируем описание
                plan_names = {
                    'notifications': '🔔 Уведомления о сериалах',
                    'recommendations': '🎯 Персональные рекомендации',
                    'tickets': '🎫 Билеты в кино',
                    'all': '📦 Все режимы'
                }
                
                period_names = {
                    'month': 'месяц',
                    '3months': '3 месяца',
                    'year': 'год',
                    'lifetime': 'навсегда',
                    'test': 'тестовый (10 минут)'
                }
            
                text = f"✏️ <b>Изменение подписки</b>\n\n"
                text += f"📋 <b>Текущая подписка:</b>\n"
                text += f"• {plan_names.get(current_plan_type, current_plan_type)}\n"
                text += f"• Период: {period_names.get(period_type, period_type)}\n"
                text += f"• Сумма: {current_price}₽\n\n"
                
                text += f"📋 <b>Новая подписка:</b>\n"
                text += f"• {plan_names.get(new_plan_type, new_plan_type)}\n"
                text += f"• Период: {period_names.get(period_type, period_type)}\n"
                text += f"• Сумма: {new_price}₽\n\n"
                
                if next_payment_date:
                    if isinstance(next_payment_date, datetime):
                        next_payment_str = next_payment_date.strftime('%d.%m.%Y')
                    else:
                        try:
                            from dateutil import parser
                            next_payment_dt = parser.parse(str(next_payment_date))
                            next_payment_str = next_payment_dt.strftime('%d.%m.%Y')
                        except:
                            next_payment_str = str(next_payment_date)
                    text += f"📅 <b>Дата следующего списания:</b> {next_payment_str}\n\n"
            
                markup = InlineKeyboardMarkup(row_width=1)
                
                # Если сумма увеличивается - предлагаем два варианта
                if upgrade_price > 0:
                    text += f"💰 <b>Доплата:</b> {upgrade_price}₽\n\n"
                    text += "Выберите вариант:\n"
                    text += f"1️⃣ <b>Оплатить сейчас и изменить сумму подписки</b> — доплатите {upgrade_price}₽, подписка изменится сразу\n"
                    text += "2️⃣ <b>Изменение суммы со следующего платежа</b> — подписка изменится без доплаты со следующего списания\n"
                    
                    markup.add(InlineKeyboardButton("1️⃣ Оплатить сейчас", callback_data=f"payment:pay_upgrade_now:{subscription_id}:{new_plan_type}"))
                    markup.add(InlineKeyboardButton("2️⃣ Со следующего платежа", callback_data=f"payment:change_from_next:{subscription_id}:{new_plan_type}"))
                else:
                    # Если сумма уменьшается или не меняется - только изменение со следующего платежа
                    text += "Подписка будет изменена со следующего списания.\n"
                    markup.add(InlineKeyboardButton("✅ Подтвердить", callback_data=f"payment:change_from_next:{subscription_id}:{new_plan_type}"))
                
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group:current"))
            
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action.startswith("change_from_next:"):
                # Изменение подписки со следующего платежа (без доплаты)
                try:
                    bot_instance.answer_callback_query(call.id)
                except:
                    pass
                
                # Парсим callback_data: payment:change_from_next:{subscription_id}:{new_plan_type}
                parts = action.split(":")
                if len(parts) < 3:
                    bot_instance.answer_callback_query(call.id, "Ошибка: неверный формат", show_alert=True)
                    return
                
                subscription_id = int(parts[1])
                new_plan_type = parts[2]
                
                # Получаем информацию о текущей подписке
                from moviebot.database.db_operations import get_subscription_by_id, update_subscription_plan_type
                sub = get_subscription_by_id(subscription_id)
                
                if not sub or sub.get('user_id') != user_id:
                    bot_instance.answer_callback_query(call.id, "Подписка не найдена", show_alert=True)
                    return
                
                current_plan_type = sub.get('plan_type')
                period_type = sub.get('period_type', 'month')
                group_size = sub.get('group_size')
                next_payment_date = sub.get('next_payment_date')
                
                # Вычисляем новую цену
                group_size_str = str(group_size) if group_size else '2'
                if new_plan_type in ['notifications', 'recommendations', 'tickets']:
                    new_price = SUBSCRIPTION_PRICES['group'][group_size_str][new_plan_type].get('month', 0)
                else:
                    new_price = SUBSCRIPTION_PRICES['group'][group_size_str][new_plan_type].get(period_type, 0)
                
                # Обновляем подписку: меняем plan_type и price, но сохраняем next_payment_date
                update_subscription_plan_type(subscription_id, new_plan_type, new_price)
                
                # Формируем сообщение
                plan_names = {
                    'notifications': '🔔 Уведомления о сериалах',
                    'recommendations': '🎯 Персональные рекомендации',
                    'tickets': '🎫 Билеты в кино',
                    'all': '📦 Все режимы'
                }
                
                if next_payment_date:
                    if isinstance(next_payment_date, datetime):
                        next_payment_str = next_payment_date.strftime('%d.%m.%Y')
                    else:
                        try:
                            from dateutil import parser
                            next_payment_dt = parser.parse(str(next_payment_date))
                            next_payment_str = next_payment_dt.strftime('%d.%m.%Y')
                        except:
                            next_payment_str = str(next_payment_date)
                else:
                    next_payment_str = "не указана"
                
                text = "✅ <b>Готово!</b>\n\n"
                text += f"Подписка будет изменена с {next_payment_str}\n\n"
                text += f"📋 <b>Было:</b> {plan_names.get(current_plan_type, current_plan_type)}\n"
                text += f"📋 <b>Будет:</b> {plan_names.get(new_plan_type, new_plan_type)}\n"
                text += f"💰 <b>Сумма списания:</b> {new_price}₽"
                
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group:current"))
                
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
            
            if action.startswith("pay_upgrade_now:"):
                # Создание платежа для обновления подписки с оплатой сейчас
                try:
                    bot_instance.answer_callback_query(call.id)
                except:
                    pass
            
                # Парсим callback_data: payment:pay_upgrade_now:{subscription_id}:{new_plan_type}
                parts = action.split(":")
                if len(parts) < 3:
                    bot_instance.answer_callback_query(call.id, "Ошибка: неверный формат", show_alert=True)
                    return
            
                subscription_id = int(parts[1])
                new_plan_type = parts[2]
            
                # Получаем информацию о текущей подписке
                from moviebot.database.db_operations import get_subscription_by_id
                sub = get_subscription_by_id(subscription_id)
            
                if not sub or sub.get('user_id') != user_id:
                    bot_instance.answer_callback_query(call.id, "Подписка не найдена", show_alert=True)
                    return
            
                current_plan_type = sub.get('plan_type')
                period_type = sub.get('period_type', 'month')
                group_size = sub.get('group_size')
                chat_id = sub.get('chat_id')
            
                # Вычисляем цену для новой подписки
                group_size_str = str(group_size) if group_size else '2'
            
                # Для отдельных функций (notifications, recommendations, tickets) доступна только месячная подписка
                # Для "all" используем текущий период подписки
                if new_plan_type in ['notifications', 'recommendations', 'tickets']:
                    # Отдельные функции - только месячная подписка
                    new_price = SUBSCRIPTION_PRICES['group'][group_size_str][new_plan_type].get('month', 0)
                    current_month_price = SUBSCRIPTION_PRICES['group'][group_size_str][current_plan_type].get('month', 0)
                    upgrade_price = new_price - current_month_price
                    upgrade_period_type = 'month'
                else:
                    # Для "all" используем текущий период
                    new_price = SUBSCRIPTION_PRICES['group'][group_size_str][new_plan_type].get(period_type, 0)
                    current_price = SUBSCRIPTION_PRICES['group'][group_size_str][current_plan_type].get(period_type, 0)
                    upgrade_price = new_price - current_price
                    upgrade_period_type = period_type
            
                if upgrade_price <= 0:
                    bot_instance.answer_callback_query(call.id, "Ошибка расчета цены", show_alert=True)
                    return
                
                # Показываем информацию о подписке перед оплатой
                next_payment_date = sub.get('next_payment_date')
                plan_names = {
                    'notifications': '🔔 Уведомления о сериалах',
                    'recommendations': '🎯 Персональные рекомендации',
                    'tickets': '🎫 Билеты в кино',
                    'all': '📦 Все режимы'
                }
                
                text = f"💳 <b>Оплата доплаты</b>\n\n"
                text += f"📋 <b>Текущая подписка:</b> {plan_names.get(current_plan_type, current_plan_type)}\n"
                text += f"📋 <b>Новая подписка:</b> {plan_names.get(new_plan_type, new_plan_type)}\n"
                if next_payment_date:
                    if isinstance(next_payment_date, datetime):
                        next_payment_str = next_payment_date.strftime('%d.%m.%Y')
                    else:
                        try:
                            from dateutil import parser
                            next_payment_dt = parser.parse(str(next_payment_date))
                            next_payment_str = next_payment_dt.strftime('%d.%m.%Y')
                        except:
                            next_payment_str = str(next_payment_date)
                    text += f"📅 <b>Дата следующего списания:</b> {next_payment_str}\n"
                text += f"\n💰 <b>Доплата:</b> {upgrade_price}₽\n\n"
                text += "После оплаты подписка будет изменена сразу."
                
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("💳 Оплатить", callback_data=f"payment:confirm_upgrade_pay:{subscription_id}:{new_plan_type}:{upgrade_price}"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:upgrade_plan:{subscription_id}:{new_plan_type}"))
                
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
            
            if action.startswith("confirm_upgrade_pay:"):
                # Подтверждение оплаты доплаты для обновления подписки
                try:
                    bot_instance.answer_callback_query(call.id)
                except:
                    pass
                
                # Парсим callback_data: payment:confirm_upgrade_pay:{subscription_id}:{new_plan_type}:{upgrade_price}
                parts = action.split(":")
                if len(parts) < 4:
                    bot_instance.answer_callback_query(call.id, "Ошибка: неверный формат", show_alert=True)
                    return
                
                subscription_id = int(parts[1])
                new_plan_type = parts[2]
                upgrade_price = float(parts[3])
                
                # Получаем информацию о текущей подписке
                from moviebot.database.db_operations import get_subscription_by_id
                sub = get_subscription_by_id(subscription_id)
            
                if not sub or sub.get('user_id') != user_id:
                    bot_instance.answer_callback_query(call.id, "Подписка не найдена", show_alert=True)
                    return
            
                current_plan_type = sub.get('plan_type')
                period_type = sub.get('period_type', 'month')
                group_size = sub.get('group_size')
                chat_id = sub.get('chat_id')
            
                # Вычисляем цену для новой подписки
                group_size_str = str(group_size) if group_size else '2'
            
                # Для отдельных функций (notifications, recommendations, tickets) доступна только месячная подписка
                # Для "all" используем текущий период подписки
                if new_plan_type in ['notifications', 'recommendations', 'tickets']:
                    upgrade_period_type = 'month'
                else:
                    upgrade_period_type = period_type
            
                # Создаем платеж для обновления подписки
                # Используем существующую логику создания платежа, но с флагом upgrade
                import uuid
                payment_id = str(uuid.uuid4())
            
                return_url = os.getenv('YOOKASSA_RETURN_URL', 'tg://resolve?domain=movie_planner_bot')
            
                metadata = {
                    "user_id": str(user_id),
                    "chat_id": str(chat_id),
                    "subscription_type": "group",
                    "plan_type": new_plan_type,
                    "period_type": upgrade_period_type,
                    "payment_id": payment_id,
                    "group_size": str(group_size) if group_size else "",
                    "upgrade_subscription_id": str(subscription_id),  # Флаг для обновления существующей подписки
                    "upgrade_from_plan": current_plan_type  # Старый тип подписки
                }
            
                # Получаем username группы
                try:
                    chat = bot_instance.get_chat(chat_id)
                    if chat.username:
                        metadata["group_username"] = chat.username
                except:
                    pass
            
                # Создаем платеж через YooKassa
                if not YOOKASSA_AVAILABLE:
                    bot_instance.answer_callback_query(call.id, "Платежная система временно недоступна", show_alert=True)
                    return
            
                try:
                    from yookassa import Configuration, Payment
                    import uuid as uuid_module
                
                    Configuration.account_id = YOOKASSA_SHOP_ID
                    Configuration.secret_key = YOOKASSA_SECRET_KEY
                
                    plan_names = {
                        'notifications': 'Уведомления о сериалах',
                        'recommendations': 'Персональные рекомендации',
                        'tickets': 'Билеты в кино',
                        'all': 'Все режимы'
                    }
                
                    description = f"Обновление групповой подписки (на {group_size} участников): {plan_names.get(new_plan_type, new_plan_type)}, период: {period_type}"
                
                    # Для всех подписок кроме lifetime добавляем save_payment_method: True
                    payment_data = {
                        "amount": {
                            "value": f"{upgrade_price:.2f}",
                            "currency": "RUB"
                        },
                        "confirmation": {
                            "type": "redirect",
                            "return_url": return_url
                        },
                        "capture": True,
                        "description": description,
                        "metadata": metadata
                    }
                    
                    # Добавляем save_payment_method для всех не-lifetime подписок
                    if period_type != 'lifetime':
                        payment_data["save_payment_method"] = True
                        logger.info(f"[YOOKASSA] save_payment_method=True добавлен для period_type={period_type} (upgrade)")
                    
                    payment = Payment.create(payment_data, str(uuid_module.uuid4()))
                
                    # Сохраняем платеж в БД
                    from moviebot.database.db_operations import save_payment
                    save_payment(
                        payment_id=payment_id,
                        yookassa_payment_id=payment.id,
                        user_id=user_id,
                        chat_id=chat_id,
                        subscription_type='group',
                        plan_type=new_plan_type,
                        period_type=period_type,
                        group_size=group_size,
                        amount=upgrade_price,
                        status='pending'
                    )
                
                    # Отправляем ссылку на оплату
                    if payment.confirmation and payment.confirmation.confirmation_url:
                        text = f"💳 <b>Оплата обновления подписки</b>\n\n"
                        text += f"💰 Сумма: <b>{upgrade_price}₽</b>\n\n"
                        text += f"Нажмите на кнопку ниже для оплаты:"
                    
                        markup = InlineKeyboardMarkup(row_width=1)
                        markup.add(InlineKeyboardButton("💳 Оплатить", url=payment.confirmation.confirmation_url))
                        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group:current"))
                    
                        bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    else:
                        bot_instance.answer_callback_query(call.id, "Ошибка создания платежа", show_alert=True)
                    
                except Exception as e:
                    logger.error(f"[PAYMENT] Ошибка создания платежа для обновления подписки: {e}", exc_info=True)
                    bot_instance.answer_callback_query(call.id, "Ошибка создания платежа", show_alert=True)
                return
        
            if action.startswith("cancel_confirm:"):
                # Финальное подтверждение отмены подписки
                subscription_id = int(action.split(":")[1])
                from moviebot.database.db_operations import get_subscription_by_id, cancel_subscription, get_user_personal_subscriptions
            
                sub = get_subscription_by_id(subscription_id)
                if not sub or sub.get('user_id') != user_id:
                    bot_instance.answer_callback_query(call.id, "Подписка не найдена", show_alert=True)
                    return
            
                subscription_type = sub.get('subscription_type', 'personal')
                
                if cancel_subscription(subscription_id, user_id):
                    bot_instance.answer_callback_query(call.id, "Подписка отменена")
                    logger.info(f"[PAYMENT CANCEL CONFIRM] Подписка {subscription_id} успешно отменена для user_id={user_id}, subscription_type={subscription_type}")
                    
                    # Обновляем сообщение с информацией о подписках
                    try:
                        if subscription_type == 'personal':
                            # Для личных подписок получаем обновленный список
                            from moviebot.database.db_operations import get_user_personal_subscriptions
                            all_subs = get_user_personal_subscriptions(user_id)
                            
                            # Фильтруем только активные подписки
                            active_subs = []
                            seen_plan_types = set()
                            now = datetime.now(pytz.UTC)
                            total_price = 0
                            
                            for active_sub in all_subs:
                                expires_at = active_sub.get('expires_at')
                                plan_type = active_sub.get('plan_type')
                                
                                # Проверяем, что подписка активна
                                is_active = False
                                if not expires_at:
                                    is_active = True
                                elif isinstance(expires_at, datetime):
                                    if expires_at.tzinfo is None:
                                        expires_at = pytz.UTC.localize(expires_at)
                                    if expires_at.tzinfo != pytz.UTC:
                                        expires_at = expires_at.astimezone(pytz.UTC)
                                    is_active = expires_at > now
                                else:
                                    try:
                                        if isinstance(expires_at, str):
                                            expires_dt = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                                            if expires_dt.tzinfo is None:
                                                expires_dt = pytz.UTC.localize(expires_dt)
                                            if expires_dt.tzinfo != pytz.UTC:
                                                expires_dt = expires_dt.astimezone(pytz.UTC)
                                            is_active = expires_dt > now
                                        else:
                                            is_active = True
                                    except:
                                        is_active = True
                                
                                # Добавляем только активные и уникальные по plan_type
                                if is_active and plan_type and plan_type not in seen_plan_types:
                                    active_subs.append(active_sub)
                                    seen_plan_types.add(plan_type)
                                    total_price += active_sub.get('price', 0)
                            
                            # Формируем сообщение
                            plan_names = {
                                'notifications': 'Уведомления о сериалах',
                                'recommendations': 'Рекомендации',
                                'tickets': 'Билеты',
                                'all': 'Все режимы'
                            }
                            
                            if active_subs:
                                if len(active_subs) == 1:
                                    plan_type = active_subs[0].get('plan_type', 'all')
                                    plan_name = plan_names.get(plan_type, plan_type)
                                    sub = active_subs[0]
                                    expires_at = sub.get('expires_at')
                                    next_payment = sub.get('next_payment_date')
                                    activated = sub.get('activated_at')
                                    
                                    text = f"👤 <b>Личная подписка</b>\n\n"
                                    text += f"📋 <b>Название подписки:</b> {plan_name}\n\n"
                                    text += f"💰 <b>Общая сумма платежа: {total_price}₽</b>\n"
                                    if activated:
                                        text += f"📅 Дата активации: <b>{activated.strftime('%d.%m.%Y') if isinstance(activated, datetime) else activated}</b>\n"
                                    if next_payment:
                                        text += f"📅 Следующее списание: <b>{next_payment.strftime('%d.%m.%Y') if isinstance(next_payment, datetime) else next_payment}</b>\n"
                                    if expires_at:
                                        text += f"⏰ Действует до: <b>{expires_at.strftime('%d.%m.%Y') if isinstance(expires_at, datetime) else expires_at}</b>\n"
                                    else:
                                        text += f"⏰ Действует: <b>Навсегда</b>\n"
                                    
                                    markup = InlineKeyboardMarkup(row_width=1)
                                    subscription_id_new = sub.get('id')
                                    if subscription_id_new and subscription_id_new > 0:
                                        markup.add(InlineKeyboardButton("✏️ Изменить подписку", callback_data=f"payment:modify:{subscription_id_new}"))
                                        markup.add(InlineKeyboardButton("❌ Отменить", callback_data=f"payment:cancel:{subscription_id_new}"))
                                    else:
                                        markup.add(InlineKeyboardButton("✏️ Изменить подписку", callback_data="payment:tariffs:personal"))
                                        markup.add(InlineKeyboardButton("❌ Отменить", callback_data="payment:cancel:personal"))
                                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active"))
                                else:
                                    text = f"👤 <b>Личная подписка</b>\n\n"
                                    text += f"📋 <b>Активные подписки:</b>\n"
                                    for active_sub in active_subs:
                                        sub_plan_type = active_sub.get('plan_type', 'all')
                                        sub_plan_name = plan_names.get(sub_plan_type, sub_plan_type)
                                        sub_price = active_sub.get('price', 0)
                                        text += f"• {sub_plan_name} ({sub_price}₽)\n"
                                    text += "\n"
                                    text += f"💰 <b>Общая сумма платежа: {total_price}₽</b>\n"
                                    
                                    markup = InlineKeyboardMarkup(row_width=1)
                                    markup.add(InlineKeyboardButton("✏️ Изменить подписку", callback_data="payment:modify:all"))
                                    for active_sub in active_subs:
                                        sub_id = active_sub.get('id')
                                        if sub_id and sub_id > 0:
                                            sub_plan_type = active_sub.get('plan_type', 'all')
                                            sub_plan_name = plan_names.get(sub_plan_type, sub_plan_type)
                                            markup.add(InlineKeyboardButton(f"❌ Отменить: {sub_plan_name}", callback_data=f"payment:cancel:{sub_id}"))
                                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active"))
                            else:
                                text = "👤 <b>Личная подписка</b>\n\n"
                                text += "❌ Активная подписка отсутствует, выберите тариф для подключения"
                                markup = InlineKeyboardMarkup(row_width=1)
                                markup.add(InlineKeyboardButton("💰 Тарифы", callback_data="payment:tariffs:personal"))
                                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active"))
                            
                            bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                        elif subscription_type == 'group':
                            # Для групповых подписок получаем информацию о групповой подписке
                            from moviebot.database.db_operations import get_subscription_members, get_active_group_users, get_active_group_subscription_by_chat_id
                            group_sub = get_active_group_subscription_by_chat_id(chat_id)
                            
                            if group_sub:
                                expires_at = group_sub.get('expires_at')
                                next_payment = group_sub.get('next_payment_date')
                                price = group_sub.get('price', 0)
                                activated = group_sub.get('activated_at')
                                group_size = group_sub.get('group_size')
                                subscription_id_new = group_sub.get('id')
                                plan_type = group_sub.get('plan_type', 'all')
                                period_type = group_sub.get('period_type', 'lifetime')
                                
                                # Получаем информацию о группе
                                try:
                                    chat = bot_instance.get_chat(chat_id)
                                    group_title = chat.title
                                    group_username = chat.username
                                except Exception as chat_error:
                                    logger.error(f"[PAYMENT] Ошибка получения информации о группе: {chat_error}")
                                    group_title = "Группа"
                                    group_username = None
                                
                                text = f"👥 <b>Групповая подписка</b>\n\n"
                                if plan_type == 'all':
                                    text += f"📦 <b>Пакетная подписка - Все режимы</b>\n\n"
                                text += f"Группа: <b>{group_title}</b>\n"
                                if group_username:
                                    text += f"@{group_username}\n"
                                text += f"\n💰 Сумма платежа: <b>{price}₽</b>\n"
                                if group_size:
                                    text += f"👥 Количество участников: <b>{group_size}</b>\n"
                                    if subscription_id_new and subscription_id_new > 0:
                                        try:
                                            members = get_subscription_members(subscription_id_new)
                                            # Исключаем бота из списка участников
                                            if BOT_ID and BOT_ID in members:
                                                members = {uid: uname for uid, uname in members.items() if uid != BOT_ID}
                                            members_count = len(members) if members else 0
                                            text += f"✅ Участников в подписке: <b>{members_count}</b>\n"
                                        except Exception as members_error:
                                            logger.error(f"[PAYMENT] Ошибка получения участников подписки: {members_error}")
                                            # Пытаемся получить количество из активных пользователей группы
                                            try:
                                                active_users = get_active_group_users(chat_id, bot_id=BOT_ID)
                                                if active_users and BOT_ID:
                                                    active_users = {uid: uname for uid, uname in active_users.items() if uid != BOT_ID}
                                                active_count = len(active_users) if active_users else 0
                                                text += f"✅ Участников в подписке: <b>{active_count}</b>\n"
                                            except Exception as active_error:
                                                logger.error(f"[PAYMENT] Ошибка получения активных пользователей: {active_error}")
                                                text += f"✅ Участников в подписке: <b>?</b>\n"
                                if activated:
                                    text += f"📅 Дата активации: <b>{activated.strftime('%d.%m.%Y') if isinstance(activated, datetime) else activated}</b>\n"
                                if next_payment:
                                    text += f"📅 Следующее списание: <b>{next_payment.strftime('%d.%m.%Y') if isinstance(next_payment, datetime) else next_payment}</b>\n"
                                if expires_at:
                                    text += f"⏰ Действует до: <b>{expires_at.strftime('%d.%m.%Y') if isinstance(expires_at, datetime) else expires_at}</b>\n"
                                else:
                                    text += f"⏰ Действует: <b>Навсегда</b>\n"
                                
                                markup = InlineKeyboardMarkup(row_width=1)
                                # Показываем кнопку списка участников только для реальных подписок (не виртуальных)
                                if subscription_id_new and subscription_id_new > 0:
                                    markup.add(InlineKeyboardButton("👥 Список участников", callback_data=f"payment:group_members:{subscription_id_new}"))
                                    
                                    # Показываем кнопку "Отписаться" только для активных участников
                                    try:
                                        members = get_subscription_members(subscription_id_new)
                                        if BOT_ID and BOT_ID in members:
                                            members = {uid: uname for uid, uname in members.items() if uid != BOT_ID}
                                        if members and user_id in members:
                                            markup.add(InlineKeyboardButton("❌ Отписаться", callback_data=f"payment:cancel:{subscription_id_new}"))
                                    except Exception as members_error:
                                        logger.error(f"[PAYMENT] Ошибка получения участников для кнопки отписки: {members_error}")
                                
                                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group"))
                            else:
                                text = "👥 <b>Групповая подписка</b>\n\n"
                                text += "❌ Активная подписка отсутствует, выберите тариф для подключения"
                                markup = InlineKeyboardMarkup(row_width=1)
                                markup.add(InlineKeyboardButton("💰 Тарифы", callback_data="payment:tariffs:group"))
                                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:active:group"))
                            
                            bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    except Exception as update_e:
                        logger.error(f"[PAYMENT CANCEL CONFIRM] Ошибка обновления информации о подписках: {update_e}", exc_info=True)
                        # Если не удалось обновить, показываем простое сообщение
                        try:
                            bot_instance.edit_message_text(
                                "✅ <b>Подписка отменена</b>\n\nВаша подписка была успешно отменена.\n\nИспользуйте /payment для просмотра информации о подписках.",
                                call.message.chat.id,
                                call.message.message_id,
                                parse_mode='HTML'
                            )
                        except Exception as e:
                            if "message is not modified" not in str(e):
                                logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                else:
                    bot_instance.answer_callback_query(call.id, "Ошибка отмены подписки", show_alert=True)
                return
        
            if action.startswith("cancel:"):
                # Подтверждение отмены
                logger.info(f"[PAYMENT CANCEL] Получен callback cancel: action={action}, user_id={user_id}, chat_id={chat_id}")
                parts = action.split(":")
                second_param = parts[1]
            
                # Проверяем, является ли второй параметр числом (subscription_id) или строкой (personal/group)
                sub_type = None
                try:
                    subscription_id = int(second_param)
                    logger.info(f"[PAYMENT CANCEL] Параметр является числом (subscription_id={subscription_id})")
                    # Это subscription_id - проверяем тип подписки
                    from moviebot.database.db_operations import get_subscription_by_id
                    sub = get_subscription_by_id(subscription_id)
                
                    if sub:
                        subscription_type = sub.get('subscription_type')
                        sub_type = subscription_type  # Сохраняем для использования ниже
                    
                        # Для групповых подписок показываем подтверждение с предложением более дешевых вариантов
                        if subscription_type == 'group':
                            plan_type = sub.get('plan_type', 'all')
                            period_type = sub.get('period_type', 'lifetime')
                            current_price = float(sub.get('price', 0))
                            group_size = sub.get('group_size', 2)
                            group_size_str = str(group_size)
                        
                            # Находим более дешевые варианты подписки
                            cheaper_options = []
                            if plan_type == 'all':
                                # Если текущая подписка "all", предлагаем отдельные функции
                                if current_price > SUBSCRIPTION_PRICES['group'][group_size_str]['notifications']['month']:
                                    cheaper_options.append(('🔔 Уведомления', SUBSCRIPTION_PRICES['group'][group_size_str]['notifications']['month'], f"payment:subscribe:group:{group_size}:notifications:month"))
                                if current_price > SUBSCRIPTION_PRICES['group'][group_size_str]['recommendations']['month']:
                                    cheaper_options.append(('🎯 Рекомендации', SUBSCRIPTION_PRICES['group'][group_size_str]['recommendations']['month'], f"payment:subscribe:group:{group_size}:recommendations:month"))
                                if current_price > SUBSCRIPTION_PRICES['group'][group_size_str]['tickets']['month']:
                                    cheaper_options.append(('🎫 Билеты', SUBSCRIPTION_PRICES['group'][group_size_str]['tickets']['month'], f"payment:subscribe:group:{group_size}:tickets:month"))
                            # Сортируем по цене
                            cheaper_options.sort(key=lambda x: x[1])
                            cheaper_options = cheaper_options[:3]  # Берем 3 самых дешевых
                        
                            bot_instance.answer_callback_query(call.id)
                        
                            # Формируем сообщение с подтверждением
                            text = "Точно хотите отменить подписку? Вы можете изменить подписку на другие варианты:\n\n"
                        
                            markup = InlineKeyboardMarkup(row_width=1)
                        
                            # Добавляем кнопки с более дешевыми вариантами
                            if cheaper_options:
                                for option_name, option_price, callback_data in cheaper_options:
                                    markup.add(InlineKeyboardButton(f"{option_name} ({option_price}₽/мес)", callback_data=callback_data))
                        
                            # Кнопки подтверждения отмены
                            markup.add(InlineKeyboardButton("❌ Точно отменить", callback_data=f"payment:cancel_confirm:{subscription_id}"))
                            markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:active:group:current"))
                        
                            try:
                                bot_instance.edit_message_text(
                                    text,
                                    call.message.chat.id,
                                    call.message.message_id,
                                    reply_markup=markup,
                                    parse_mode='HTML'
                                )
                            except Exception as e:
                                if "message is not modified" not in str(e):
                                    logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                        else:
                            # Для личных подписок показываем подтверждение с предложением более дешевых вариантов
                            plan_type = sub.get('plan_type', 'all')
                            period_type = sub.get('period_type', 'lifetime')
                            current_price = float(sub.get('price', 0))
                            subscription_type = sub.get('subscription_type', 'personal')
                        
                            # Находим более дешевые варианты подписки
                            cheaper_options = []
                            if subscription_type == 'personal':
                                # Для личных подписок ищем более дешевые варианты
                                if plan_type == 'all':
                                    # Если текущая подписка "all", предлагаем отдельные функции
                                    if current_price > SUBSCRIPTION_PRICES['personal']['notifications']['month']:
                                        cheaper_options.append(('🔔 Уведомления', SUBSCRIPTION_PRICES['personal']['notifications']['month'], f"payment:subscribe:personal:notifications:month"))
                                    if current_price > SUBSCRIPTION_PRICES['personal']['recommendations']['month']:
                                        cheaper_options.append(('🎯 Рекомендации', SUBSCRIPTION_PRICES['personal']['recommendations']['month'], f"payment:subscribe:personal:recommendations:month"))
                                    if current_price > SUBSCRIPTION_PRICES['personal']['tickets']['month']:
                                        cheaper_options.append(('🎫 Билеты', SUBSCRIPTION_PRICES['personal']['tickets']['month'], f"payment:subscribe:personal:tickets:month"))
                                # Сортируем по цене
                                cheaper_options.sort(key=lambda x: x[1])
                                cheaper_options = cheaper_options[:3]  # Берем 3 самых дешевых
                        
                            bot_instance.answer_callback_query(call.id)
                        
                            # Формируем сообщение с подтверждением
                            text = "Точно хотите отменить подписку? Вы можете изменить подписку на другие варианты:\n\n"
                        
                            markup = InlineKeyboardMarkup(row_width=1)
                        
                            # Добавляем кнопки с более дешевыми вариантами
                            if cheaper_options:
                                for option_name, option_price, callback_data in cheaper_options:
                                    markup.add(InlineKeyboardButton(f"{option_name} ({option_price}₽/мес)", callback_data=callback_data))
                        
                            # Кнопки подтверждения отмены
                            markup.add(InlineKeyboardButton("❌ Точно отменить", callback_data=f"payment:cancel_confirm:{subscription_id}"))
                            markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:active:{subscription_type}"))
                        
                            try:
                                bot_instance.edit_message_text(
                                    text,
                                    call.message.chat.id,
                                    call.message.message_id,
                                    reply_markup=markup,
                                    parse_mode='HTML'
                                )
                            except Exception as e:
                                if "message is not modified" not in str(e):
                                    logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                    else:
                        bot_instance.answer_callback_query(call.id, "Подписка не найдена", show_alert=True)
                        return
                except ValueError:
                    # Это строка (personal/group) - используем старую логику
                    logger.info(f"[PAYMENT CANCEL] Параметр является строкой (sub_type={second_param})")
                    sub_type = second_param
                
                # Если sub_type не определен, не можем продолжить
                if not sub_type:
                    bot_instance.answer_callback_query(call.id, "Ошибка: не удалось определить тип подписки", show_alert=True)
                    return
                
                sub = get_active_subscription(chat_id, user_id, sub_type)
            
                if not sub:
                    bot_instance.answer_callback_query(call.id, "Активная подписка не найдена", show_alert=True)
                    return
            
                sub_id = sub.get('id')
                logger.info(f"[PAYMENT CANCEL] Обработка отмены подписки: sub_type={sub_type}, sub_id={sub_id}, user_id={user_id}, chat_id={chat_id}")
            
                # Для виртуальных подписок (id <= 0) просто деактивируем их в БД, если они есть
                if not sub_id or sub_id <= 0:
                    logger.info(f"[PAYMENT CANCEL] Виртуальная подписка (id={sub_id}), деактивируем через UPDATE по chat_id и user_id")
                    # Для виртуальных подписок деактивируем все активные подписки этого типа для пользователя
                    # Используем глобальные cursor и conn, которые уже определены в начале файла
                    with db_lock:
                        cursor.execute("""
                            UPDATE subscriptions 
                            SET is_active = FALSE, cancelled_at = NOW()
                            WHERE chat_id = %s AND user_id = %s AND subscription_type = %s AND is_active = TRUE
                        """, (chat_id, user_id, sub_type))
                        conn.commit()
                        rows_updated = cursor.rowcount
                
                    if rows_updated > 0:
                        logger.info(f"[PAYMENT CANCEL] Деактивировано {rows_updated} виртуальных подписок")
                    bot_instance.answer_callback_query(call.id, "Подписка отменена")
                    try:
                        bot_instance.edit_message_text(
                            f"✅ <b>Подписка отменена</b>\n\nВаша {sub_type} подписка была успешно отменена.",
                            call.message.chat.id,
                            call.message.message_id,
                            parse_mode='HTML'
                        )
                    except Exception as e:
                        if "message is not modified" not in str(e):
                            logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                else:
                    logger.info(f"[PAYMENT CANCEL] Виртуальная подписка не найдена в БД для деактивации")
                    bot_instance.answer_callback_query(call.id, "Виртуальная подписка уже отменена", show_alert=True)
                    return
            
                # Для реальных подписок (id > 0) показываем подтверждение с предложением более дешевых вариантов
                plan_type = sub.get('plan_type', 'all')
                period_type = sub.get('period_type', 'lifetime')
                current_price = float(sub.get('price', 0))
                subscription_type = sub.get('subscription_type', sub_type)
            
                # Находим более дешевые варианты подписки
                cheaper_options = []
                if subscription_type == 'personal':
                    if plan_type == 'all':
                        if current_price > SUBSCRIPTION_PRICES['personal']['notifications']['month']:
                            cheaper_options.append(('🔔 Уведомления', SUBSCRIPTION_PRICES['personal']['notifications']['month'], f"payment:subscribe:personal:notifications:month"))
                        if current_price > SUBSCRIPTION_PRICES['personal']['recommendations']['month']:
                            cheaper_options.append(('🎯 Рекомендации', SUBSCRIPTION_PRICES['personal']['recommendations']['month'], f"payment:subscribe:personal:recommendations:month"))
                        if current_price > SUBSCRIPTION_PRICES['personal']['tickets']['month']:
                            cheaper_options.append(('🎫 Билеты', SUBSCRIPTION_PRICES['personal']['tickets']['month'], f"payment:subscribe:personal:tickets:month"))
                elif subscription_type == 'group':
                    group_size = sub.get('group_size', 2)
                    group_size_str = str(group_size)
                    if plan_type == 'all':
                        if current_price > SUBSCRIPTION_PRICES['group'][group_size_str]['notifications']['month']:
                            cheaper_options.append(('🔔 Уведомления', SUBSCRIPTION_PRICES['group'][group_size_str]['notifications']['month'], f"payment:subscribe:group:{group_size}:notifications:month"))
                        if current_price > SUBSCRIPTION_PRICES['group'][group_size_str]['recommendations']['month']:
                            cheaper_options.append(('🎯 Рекомендации', SUBSCRIPTION_PRICES['group'][group_size_str]['recommendations']['month'], f"payment:subscribe:group:{group_size}:recommendations:month"))
                        if current_price > SUBSCRIPTION_PRICES['group'][group_size_str]['tickets']['month']:
                            cheaper_options.append(('🎫 Билеты', SUBSCRIPTION_PRICES['group'][group_size_str]['tickets']['month'], f"payment:subscribe:group:{group_size}:tickets:month"))
            
                # Сортируем по цене
                cheaper_options.sort(key=lambda x: x[1])
                cheaper_options = cheaper_options[:3]  # Берем 3 самых дешевых
            
                bot_instance.answer_callback_query(call.id)
            
                # Формируем сообщение с подтверждением
                text = "Точно хотите отменить подписку? Вы можете изменить подписку на другие варианты:\n\n"
            
                markup = InlineKeyboardMarkup(row_width=1)
            
                # Добавляем кнопки с более дешевыми вариантами
                if cheaper_options:
                    for option_name, option_price, callback_data in cheaper_options:
                        markup.add(InlineKeyboardButton(f"{option_name} ({option_price}₽/мес)", callback_data=callback_data))
            
                # Кнопки подтверждения отмены
                markup.add(InlineKeyboardButton("❌ Точно отменить", callback_data=f"payment:cancel_confirm:{sub_id}"))
                back_callback = f"payment:active:{subscription_type}" if subscription_type == 'personal' else "payment:active:group:current"
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data=back_callback))
            
                try:
                    bot_instance.edit_message_text(
                        text,
                        call.message.chat.id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
        
            if action == "back":
                # Возврат в главное меню оплаты
                personal_sub = get_active_subscription(chat_id, user_id, 'personal')
                group_sub = get_active_subscription(chat_id, user_id, 'group')
            
                # Проверяем, есть ли реальные подписки (не виртуальные, id > 0)
                has_real_subscription = False
                if personal_sub:
                    sub_id = personal_sub.get('id')
                    if sub_id is not None and sub_id > 0:
                        has_real_subscription = True
                if group_sub:
                    sub_id = group_sub.get('id')
                    if sub_id is not None and sub_id > 0:
                        has_real_subscription = True
            
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("📋 Действующая подписка", callback_data="payment:active"))
                markup.add(InlineKeyboardButton("💰 Тарифы", callback_data="payment:tariffs"))
                if has_real_subscription:
                    markup.add(InlineKeyboardButton("❌ Отписаться", callback_data="payment:cancel"))
                markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            
                try:
                    bot_instance.edit_message_text(
                        "💳 <b>Оплата подписки</b>\n\nВыберите действие:",
                        call.message.chat.id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
            
            if action == "promo" or action.startswith("promo:"):
                # Обработка нажатия на кнопку "🏷️ Промокод"
                # Поддерживаем оба формата: новый (короткий) и старый (длинный)
                try:
                    bot_instance.answer_callback_query(call.id)
                    user_id = call.from_user.id
                    chat_id = call.message.chat.id
                    
                    # Получаем данные из состояния или парсим из callback_data
                    if user_id not in user_promo_state:
                        # Если состояния нет, проверяем старый формат callback_data
                        if action.startswith("promo:"):
                            # Парсим данные из старого формата: promo:group:2:notifications:month:250ad2b2:80
                            parts = action.split(":")
                            if len(parts) >= 7:
                                sub_type = parts[1]
                                group_size_str = parts[2] if parts[2] else ''
                                group_size = int(group_size_str) if group_size_str and group_size_str.isdigit() else None
                                plan_type = parts[3]
                                period_type = parts[4]
                                payment_id = parts[5] if len(parts) > 5 else ''
                                original_price = float(parts[6]) if len(parts) > 6 else 0
                                
                                # Сохраняем в состояние
                                user_promo_state[user_id] = {
                                    'chat_id': chat_id,
                                    'message_id': call.message.message_id,
                                    'sub_type': sub_type,
                                    'plan_type': plan_type,
                                    'period_type': period_type,
                                    'group_size': group_size,
                                    'payment_id': payment_id,
                                    'original_price': original_price
                                }
                            else:
                                # Если не удалось распарсить, пытаемся получить из payment_state
                                payment_state = user_payment_state.get(user_id, {})
                                if not payment_state:
                                    bot_instance.answer_callback_query(call.id, "❌ Ошибка: состояние не найдено", show_alert=True)
                                    return
                                
                                # Создаем состояние из payment_state
                                user_promo_state[user_id] = {
                                    'chat_id': chat_id,
                                    'message_id': call.message.message_id,
                                    'sub_type': payment_state.get('sub_type'),
                                    'plan_type': payment_state.get('plan_type'),
                                    'period_type': payment_state.get('period_type'),
                                    'group_size': payment_state.get('group_size'),
                                    'payment_id': payment_state.get('payment_id', ''),
                                    'original_price': payment_state.get('price', 0)
                                }
                        else:
                            # Новый формат - пытаемся получить из payment_state
                            payment_state = user_payment_state.get(user_id, {})
                            if not payment_state:
                                bot_instance.answer_callback_query(call.id, "❌ Ошибка: состояние не найдено", show_alert=True)
                                return
                            
                            # Создаем состояние из payment_state
                            user_promo_state[user_id] = {
                                'chat_id': chat_id,
                                'message_id': call.message.message_id,
                                'sub_type': payment_state.get('sub_type'),
                                'plan_type': payment_state.get('plan_type'),
                                'period_type': payment_state.get('period_type'),
                                'group_size': payment_state.get('group_size'),
                                'payment_id': payment_state.get('payment_id', ''),
                                'original_price': payment_state.get('price', 0)
                            }
                    
                    promo_state = user_promo_state[user_id]
                    sub_type = promo_state.get('sub_type')
                    group_size = promo_state.get('group_size')
                    plan_type = promo_state.get('plan_type')
                    period_type = promo_state.get('period_type')
                    payment_id = promo_state.get('payment_id', '')
                    original_price = promo_state.get('original_price', 0)
                    
                    # Отправляем сообщение с запросом промокода
                    # Используем короткий callback_data, так как данные уже сохранены в user_promo_state
                    text = "Введите промокод в ответном сообщении:"
                    markup = InlineKeyboardMarkup()
                    # Используем короткий callback_data (лимит Telegram - 64 байта)
                    callback_data_back = "payment:back_from_promo"
                    if len(callback_data_back.encode('utf-8')) > 64:
                        logger.error(f"[PROMO] ❌ callback_data слишком длинный: {len(callback_data_back)} байт")
                        # Отправляем без кнопки, если callback_data слишком длинный
                        msg = bot_instance.send_message(chat_id, text)
                    else:
                        markup.add(InlineKeyboardButton("◀️ Назад", callback_data=callback_data_back))
                        try:
                            msg = bot_instance.send_message(chat_id, text, reply_markup=markup)
                        except Exception as send_e:
                            logger.error(f"[PROMO] Ошибка отправки сообщения с кнопкой: {send_e}", exc_info=True)
                            # Пробуем отправить без кнопки
                            msg = bot_instance.send_message(chat_id, text)
                    logger.info(f"[PROMO] Запрос промокода: user_id={user_id}, payment_id={payment_id}")
                    
                except Exception as e:
                    logger.error(f"[PROMO] Ошибка при запросе промокода: {e}", exc_info=True)
                    bot_instance.answer_callback_query(call.id, "Ошибка обработки", show_alert=True)
                return
            
            if action == "back_from_promo":
                # Возврат к сообщению с кнопками оплаты
                try:
                    bot_instance.answer_callback_query(call.id)
                    user_id = call.from_user.id
                    chat_id = call.message.chat.id
                    
                    # Получаем данные из состояния промокода (вместо парсинга из callback_data)
                    if user_id not in user_promo_state:
                        bot_instance.answer_callback_query(call.id, "❌ Состояние не найдено", show_alert=True)
                        return
                    
                    promo_state = user_promo_state[user_id]
                    sub_type = promo_state.get('sub_type')
                    group_size = promo_state.get('group_size')
                    plan_type = promo_state.get('plan_type')
                    period_type = promo_state.get('period_type')
                    payment_id = promo_state.get('payment_id', '')
                    original_price = promo_state.get('original_price', 0)
                    
                    # Удаляем состояние промокода
                    del user_promo_state[user_id]
                    
                    # Восстанавливаем сообщение с кнопками оплаты
                    period_names = {
                        'month': 'месяц',
                        '3months': '3 месяца',
                        'year': 'год',
                        'lifetime': 'навсегда'
                    }
                    period_name = period_names.get(period_type, period_type)
                    
                    plan_names = {
                        'notifications': 'Уведомления о сериалах',
                        'recommendations': 'Персональные рекомендации',
                        'tickets': 'Билеты в кино',
                        'all': 'Все режимы'
                    }
                    plan_name = plan_names.get(plan_type, plan_type)
                    
                    subscription_type_name = 'Личная подписка' if sub_type == 'personal' else f'Групповая подписка (на {group_size} участников)'
                    
                    text = f"💳 <b>Оплата подписки</b>\n\n"
                    text += f"📋 <b>Выбранный тариф:</b>\n"
                    if sub_type == 'personal':
                        text += f"👤 Личная подписка\n"
                    else:
                        text += f"👥 Групповая подписка (на {group_size} участников)\n"
                    text += f"{plan_name}\n"
                    text += f"⏰ Период: {period_name}\n"
                    text += f"💰 Сумма: <b>{original_price}₽</b>\n\n"
                    
                    # Добавляем информационное сообщение для всех тарифов, кроме "навсегда"
                    if period_type != 'lifetime':
                        text += "ℹ️ После оформления подписки, данные карты будут сохранены для проведения списаний по выбранному расписанию. В дальнейшем, подтверждать отдельно платежи не придется. Вы сможете отменить подписку в любой момент\n\n"
                    
                    text += "Нажмите кнопку ниже для перехода к оплате:"
                    
                    stars_amount = rubles_to_stars(original_price)
                    
                    markup = InlineKeyboardMarkup(row_width=1)
                    # Если есть payment_id, значит уже создан платеж YooKassa
                    if payment_id and len(payment_id) > 8:
                        # Получаем URL из платежа
                        from moviebot.database.db_operations import get_payment_by_id
                        payment_data = get_payment_by_id(payment_id)
                        if payment_data and payment_data.get('yookassa_payment_id'):
                            from yookassa import Payment, Configuration
                            # Используем глобальные переменные, импортированные в начале файла
                            # НЕ импортируем локально, чтобы избежать UnboundLocalError
                            Configuration.account_id = YOOKASSA_SHOP_ID
                            Configuration.secret_key = YOOKASSA_SECRET_KEY
                            try:
                                yookassa_payment = Payment.find_one(payment_data['yookassa_payment_id'])
                                confirmation_url = yookassa_payment.confirmation.confirmation_url
                                markup.add(InlineKeyboardButton("💳 Оплатить", url=confirmation_url))
                            except:
                                pass
                    
                    callback_data_stars = f"payment:pay_stars:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}:{payment_id}"
                    markup.add(InlineKeyboardButton(f"⭐ Оплатить звездами Telegram ({stars_amount}⭐)", callback_data=callback_data_stars))
                    # Добавляем кнопку промокода
                    # Сохраняем данные в состояние для использования короткого callback_data
                    user_id = call.from_user.id
                    user_promo_state[user_id] = {
                        'chat_id': chat_id,
                        'message_id': call.message.message_id,
                        'sub_type': sub_type,
                        'plan_type': plan_type,
                        'period_type': period_type,
                        'group_size': group_size,
                        'payment_id': payment_id,
                        'original_price': original_price
                    }
                    # Используем короткий callback_data
                    callback_data_promo = "payment:promo"
                    markup.add(InlineKeyboardButton("🏷️ Промокод", callback_data=callback_data_promo))
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:subscribe:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}" if group_size else f"payment:subscribe:{sub_type}:{plan_type}:{period_type}"))
                    
                    try:
                        bot_instance.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    except:
                        bot_instance.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
                    
                except Exception as e:
                    logger.error(f"[PROMO] Ошибка при возврате: {e}", exc_info=True)
                return
        
        except Exception as e:
            logger.error(f"❌ Ошибка в handle_payment_callback: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
            except:
                pass


@bot.message_handler(content_types=['successful_payment'])
def handle_successful_payment(message):
    """Обработчик успешного платежа через Telegram Stars"""
    try:
        logger.info(f"[SUCCESSFUL PAYMENT] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
        
        if not message.successful_payment:
            logger.warning(f"[SUCCESSFUL PAYMENT] successful_payment отсутствует в сообщении")
            return
        
        successful_payment = message.successful_payment
        invoice_payload = successful_payment.invoice_payload
        telegram_payment_charge_id = getattr(successful_payment, 'telegram_payment_charge_id', None)
        
        logger.info(f"[SUCCESSFUL PAYMENT] invoice_payload={invoice_payload}, telegram_payment_charge_id={telegram_payment_charge_id}")
        
        if not invoice_payload:
            logger.warning(f"[SUCCESSFUL PAYMENT] invoice_payload отсутствует")
            return
        
        # Парсим payment_id из invoice_payload (формат: stars_{payment_id})
        if not invoice_payload.startswith('stars_'):
            logger.warning(f"[SUCCESSFUL PAYMENT] Неверный формат invoice_payload: {invoice_payload}")
            return
        
        payment_id = invoice_payload.replace('stars_', '', 1)
        logger.info(f"[SUCCESSFUL PAYMENT] Извлечен payment_id: {payment_id}")
        
        # Обновляем платеж в БД, добавляя telegram_payment_charge_id
        if telegram_payment_charge_id:
            with db_lock:
                cursor.execute("""
                    UPDATE payments 
                    SET telegram_payment_charge_id = %s, status = 'succeeded', updated_at = NOW()
                    WHERE payment_id = %s
                """, (telegram_payment_charge_id, payment_id))
                conn.commit()
                logger.info(f"[SUCCESSFUL PAYMENT] ✅ Обновлен платеж: payment_id={payment_id}, telegram_payment_charge_id={telegram_payment_charge_id[:50]}...")
        else:
            logger.warning(f"[SUCCESSFUL PAYMENT] telegram_payment_charge_id отсутствует в successful_payment")
            # Обновляем статус платежа на succeeded даже без charge_id
            with db_lock:
                cursor.execute("""
                    UPDATE payments 
                    SET status = 'succeeded', updated_at = NOW()
                    WHERE payment_id = %s
                """, (payment_id,))
                conn.commit()
        
        # Получаем данные платежа для создания подписки
        with db_lock:
            cursor.execute("""
                SELECT user_id, chat_id, subscription_type, plan_type, period_type, group_size, amount
                FROM payments 
                WHERE payment_id = %s
            """, (payment_id,))
            payment_row = cursor.fetchone()
        
        if not payment_row:
            logger.error(f"[SUCCESSFUL PAYMENT] Платеж {payment_id} не найден в БД")
            return
        
        if isinstance(payment_row, dict):
            user_id = payment_row.get('user_id')
            chat_id = payment_row.get('chat_id')
            subscription_type = payment_row.get('subscription_type')
            plan_type = payment_row.get('plan_type')
            period_type = payment_row.get('period_type')
            group_size = payment_row.get('group_size')
            amount = payment_row.get('amount')
        else:
            user_id = payment_row[0]
            chat_id = payment_row[1]
            subscription_type = payment_row[2]
            plan_type = payment_row[3]
            period_type = payment_row[4]
            group_size = payment_row[5] if len(payment_row) > 5 else None
            amount = payment_row[6] if len(payment_row) > 6 else 0
        
        logger.info(f"[SUCCESSFUL PAYMENT] Данные платежа: user_id={user_id}, chat_id={chat_id}, subscription_type={subscription_type}, plan_type={plan_type}, period_type={period_type}")
        
        # Получаем username пользователя
        telegram_username = None
        if message.from_user:
            telegram_username = message.from_user.username
        
        # Создаем/продлеваем подписку (логика из payment_callbacks.py)
        from moviebot.scheduler import send_successful_payment_notification
        from moviebot.database.db_operations import get_active_subscription, renew_subscription, add_subscription_member, create_subscription
        
        # Проверяем, есть ли уже активная подписка с такими же параметрами
        existing_sub = get_active_subscription(chat_id, user_id, subscription_type)
        
        subscription_id = None
        if existing_sub and existing_sub.get('id') and existing_sub.get('id') > 0:
            # Проверяем, совпадают ли параметры подписки
            existing_plan = existing_sub.get('plan_type')
            existing_period = existing_sub.get('period_type')
            existing_group_size = existing_sub.get('group_size')
            
            # Если параметры совпадают, продлеваем подписку
            if (existing_plan == plan_type and 
                existing_period == period_type and 
                (subscription_type != 'group' or existing_group_size == group_size)):
                subscription_id = existing_sub.get('id')
                # Продлеваем подписку
                renew_subscription(subscription_id, period_type)
                logger.info(f"[SUCCESSFUL PAYMENT] Подписка {subscription_id} продлена через Stars")
            else:
                # Параметры не совпадают - создаем новую подписку
                subscription_id = create_subscription(
                    chat_id=chat_id,
                    user_id=user_id,
                    subscription_type=subscription_type,
                    plan_type=plan_type,
                    period_type=period_type,
                    price=amount,
                    telegram_username=telegram_username,
                    group_username=None,  # Для Stars подписок group_username не нужен
                    group_size=group_size,
                    payment_method_id=None  # Для Stars подписок payment_method_id = None (Telegram управляет списаниями)
                )
                logger.info(f"[SUCCESSFUL PAYMENT] Создана новая подписка {subscription_id} через Stars")
                
                # Автоматически добавляем оплатившего пользователя в групповую подписку
                if subscription_id and subscription_type == 'group':
                    try:
                        add_subscription_member(subscription_id, user_id, telegram_username)
                        logger.info(f"[SUCCESSFUL PAYMENT] Оплативший пользователь {user_id} (@{telegram_username}) автоматически добавлен в подписку {subscription_id}")
                    except Exception as add_error:
                        logger.error(f"[SUCCESSFUL PAYMENT] Ошибка при автоматическом добавлении оплатившего пользователя: {add_error}", exc_info=True)
        else:
            # Нет активной подписки - создаем новую
            subscription_id = create_subscription(
                chat_id=chat_id,
                user_id=user_id,
                subscription_type=subscription_type,
                plan_type=plan_type,
                period_type=period_type,
                price=amount,
                telegram_username=telegram_username,
                group_username=None,  # Для Stars подписок group_username не нужен
                group_size=group_size,
                payment_method_id=None  # Для Stars подписок payment_method_id = None (Telegram управляет списаниями)
            )
            logger.info(f"[SUCCESSFUL PAYMENT] Создана новая подписка {subscription_id} через Stars")
            
            # Автоматически добавляем оплатившего пользователя в групповую подписку
            if subscription_id and subscription_type == 'group':
                try:
                    add_subscription_member(subscription_id, user_id, telegram_username)
                    logger.info(f"[SUCCESSFUL PAYMENT] Оплативший пользователь {user_id} (@{telegram_username}) автоматически добавлен в подписку {subscription_id}")
                except Exception as add_error:
                    logger.error(f"[SUCCESSFUL PAYMENT] Ошибка при автоматическом добавлении оплатившего пользователя: {add_error}", exc_info=True)
        
        if subscription_id:
            logger.info(f"[SUCCESSFUL PAYMENT] ✅ Подписка создана: subscription_id={subscription_id}")
            
            # Отправляем уведомление об успешной оплате
            send_successful_payment_notification(
                chat_id=chat_id,
                subscription_id=subscription_id,
                subscription_type=subscription_type,
                plan_type=plan_type,
                period_type=period_type
            )
        else:
            logger.error(f"[SUCCESSFUL PAYMENT] ❌ Не удалось создать подписку для payment_id={payment_id}")
        
        logger.info(f"[SUCCESSFUL PAYMENT] ===== END: успешно обработан")
        
    except Exception as e:
        logger.error(f"[SUCCESSFUL PAYMENT] ❌ Ошибка обработки successful_payment: {e}", exc_info=True)

