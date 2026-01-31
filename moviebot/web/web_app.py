"""
Flask приложение для webhook
"""
from flask import Flask, request, jsonify, abort
import logging
import telebot
import os
import sys
import time
import threading

# Импорт yookassa удален, используется moviebot.api.yookassa_api
from dotenv import load_dotenv
from moviebot.services.shazam_service import init_shazam_index

# Загружаем переменные окружения из .env файла (для локальной разработки)
# В Railway переменные окружения уже доступны через os.getenv()
load_dotenv()

logger = logging.getLogger(__name__)

werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.WARNING)

flask_logger = logging.getLogger('flask')
flask_logger.setLevel(logging.WARNING)

# Отключаем логирование Flask встроенным способом
#app.logger.disabled = True

# Добавляем логирование при создании приложения
#logger.info("[WEB APP] Flask приложение создано")

# Глобальное логирование всех запросов - ПРИНУДИТЕЛЬНОЕ

# ============================================================================
# ⚠️ КРИТИЧНО ДЛЯ RAILWAY: ФУНКЦИЯ create_web_app
# ============================================================================
# Эта функция создает Flask приложение с webhook обработчиками
# ОБЯЗАТЕЛЬНО должна возвращать app для запуска на Railway
# НЕ МЕНЯТЬ сигнатуру функции и логику возврата app!
# ============================================================================
def check_environment_variables():
    """Проверяет наличие необходимых переменных окружения"""
    nalog_inn = os.getenv('NALOG_INN')
    nalog_password = os.getenv('NALOG_PASSWORD')
   
    logger.info("=" * 80)
    logger.info("[WEB APP] Проверка переменных окружения при старте:")
    logger.info(f"[WEB APP] NALOG_INN: {'✅ установлен' if nalog_inn and nalog_inn.strip() else '❌ НЕ УСТАНОВЛЕН'}")
    logger.info(f"[WEB APP] NALOG_PASSWORD: {'✅ установлен' if nalog_password and nalog_password.strip() else '❌ НЕ УСТАНОВЛЕН'}")
   
    if not nalog_inn or not nalog_password or not nalog_inn.strip() or not nalog_password.strip():
        logger.warning("[WEB APP] ⚠️ NALOG_INN или NALOG_PASSWORD не настроены - создание чеков будет недоступно")
    else:
        logger.info("[WEB APP] ✅ Все переменные для создания чеков настроены")
    logger.info("=" * 80)

# Вызываем проверку при импорте модуля
check_environment_variables()

def create_web_app(bot):
    app = Flask(__name__)
    app.logger.disabled = True  # отключаем дефолтный логгер Flask

    logger.info("[WEB APP] Flask app создан внутри create_web_app")

    # Гарантируем наличие таблиц (в т.ч. site_sessions) при любом способе запуска (main.py или gunicorn)
    try:
        from moviebot.database.db_connection import init_database
        init_database()
        logger.info("[WEB APP] init_database() выполнен (таблицы site_sessions и др. проверены)")
    except Exception as e:
        logger.warning(f"[WEB APP] init_database при старте: {e}", exc_info=True)

    @app.route('/webhook', methods=['POST', 'GET'])
    def webhook():
        logger.info("=" * 80)
        logger.info(f"[WEBHOOK] ===== ПОЛУЧЕН ЗАПРОС ({request.method}) =====")
        logger.info(f"[WEBHOOK] IP: {request.remote_addr}")
        logger.info(f"[WEBHOOK] Content-Type: {request.headers.get('content-type')}")

        if request.method == 'GET':
            logger.info("[WEBHOOK] GET-запрос — возвращаем 200 для проверки")
            return '', 200

        if request.headers.get('content-type') != 'application/json':
            logger.warning("[WEBHOOK] Неверный Content-Type")
            abort(400)

        try:
            json_string = request.get_data().decode('utf-8')
            logger.info(f"[WEBHOOK] JSON получен, размер: {len(json_string)} байт")
            logger.info(f"[WEBHOOK] JSON preview (первые 1000 символов): {json_string[:1000]}...")

            update = telebot.types.Update.de_json(json_string)
            if update:
                logger.info(f"[WEBHOOK] Update ID: {update.update_id}")
                
                # Логируем тип обновления для отладки платежей
                if update.message:
                    if update.message.successful_payment:
                        logger.info("[WEBHOOK] ⭐ ОБНАРУЖЕН successful_payment! ⭐")
                    if update.message.content_type == 'successful_payment':
                        logger.info("[WEBHOOK] ⭐ ОБНАРУЖЕН successful_payment (content_type)! ⭐")
                if update.pre_checkout_query:
                    logger.info("[WEBHOOK] PRE_CHECKOUT_QUERY пришел! (хотя для Stars не должен)")
                
                # КРИТИЧНО: Обрабатываем update в отдельном потоке, чтобы сразу вернуть 200
                # Это предотвращает 499 ошибки (timeout) от Telegram
                def process_update_async():
                    try:
                        bot.process_new_updates([update])
                        logger.info("[WEBHOOK] ✅ Обновление успешно обработано")
                    except Exception as process_e:
                        logger.error(f"[WEBHOOK] Ошибка при обработке update: {process_e}", exc_info=True)
                
                # Запускаем обработку в отдельном потоке
                process_thread = threading.Thread(target=process_update_async, daemon=True)
                process_thread.start()
                logger.info("[WEBHOOK] Обработка update запущена в фоновом потоке")
            else:
                logger.warning("[WEBHOOK] Update не распарсился")
        except Exception as e:
            logger.error(f"[WEBHOOK] Ошибка обработки обновления: {e}", exc_info=True)
            # Telegram требует 200 даже при ошибке
            return '', 200

        # ВАЖНО: Возвращаем 200 СРАЗУ, не дожидаясь обработки
        # Это предотвращает 499 ошибки (client closed connection)
        return '', 200
    
    def process_yookassa_notification(event_json, is_test=False):
        """Обрабатывает уведомление от ЮKassa (можно вызывать из webhook или теста)"""
        try:
            logger.info("=" * 80)
            logger.info(f"[YOOKASSA] ===== ОБРАБОТКА СОБЫТИЯ =====")
            logger.info(f"[YOOKASSA] Событие: {event_json.get('event')} (тест: {is_test})")
            logger.info(f"[YOOKASSA] Полный JSON: {event_json}")
            
            if event_json.get('event') == 'payment.succeeded':
                payment_id = event_json.get('object', {}).get('id')
                if not payment_id:
                    logger.warning(f"[YOOKASSA] Платеж успешен, но payment_id отсутствует в объекте")
                    return jsonify({'status': 'error', 'message': 'Payment ID not found'}), 400
                
                logger.info(f"[YOOKASSA] Платеж успешен: {payment_id}")
                
                from moviebot.database.db_operations import get_payment_by_yookassa_id, update_payment_status
                from moviebot.api.yookassa_api import get_payment_info
                
                payment_data = get_payment_by_yookassa_id(payment_id)
                if not payment_data:
                    logger.warning(f"[YOOKASSA] Платеж {payment_id} не найден в БД")
                    return jsonify({'status': 'ok', 'message': 'Payment not found in DB'}), 200
                
                logger.info(f"[YOOKASSA] Платеж найден в БД: payment_id={payment_data.get('payment_id')}, status={payment_data.get('status')}")
                
                # Импортируем функции для работы с подписками
                from moviebot.database.db_operations import create_subscription, add_subscription_member
                
                payment = None
                payment_status = None
                if not is_test:
                    try:
                        payment = get_payment_info(payment_id)
                        payment_status = payment.status if payment else None
                    except Exception as e:
                        logger.error(f"[YOOKASSA] Ошибка получения платежа из ЮKassa: {e}", exc_info=True)
                        payment_status = 'succeeded'
                else:
                    payment_status = 'succeeded'
                
                db_status = payment_data.get('status')
                if payment_status == 'succeeded' and db_status != 'succeeded':
                    logger.info(f"[YOOKASSA] Платеж успешен, обрабатываем подписку")
                    
                    # Получаем полный объект платежа для проверки saved
                    from yookassa import Payment
                    try:
                        full_payment = Payment.find_one(payment_id)
                        logger.info("[YOOKASSA] Полный объект платежа получен")
                    except Exception as e:
                        logger.error(f"[YOOKASSA] Ошибка получения полного объекта: {e}")
                        full_payment = None
                    
                    # Обновляем статус сразу
                    update_payment_status(payment_data['payment_id'], 'succeeded')
                    
                    # Метаданные
                    if payment and hasattr(payment, 'metadata') and payment.metadata:
                        metadata = payment.metadata
                    elif is_test and event_json.get('object', {}).get('metadata'):
                        metadata = event_json.get('object', {}).get('metadata', {})
                    else:
                        metadata = {}
                    
                    user_id = int(metadata.get('user_id', payment_data['user_id']))
                    chat_id = int(metadata.get('chat_id', payment_data['chat_id']))
                    subscription_type = metadata.get('subscription_type', payment_data['subscription_type'])
                    plan_type = metadata.get('plan_type', payment_data['plan_type'])
                    period_type = metadata.get('period_type', payment_data['period_type'])
                    
                    group_size = None
                    if metadata.get('group_size'):
                        try:
                            group_size = int(metadata.get('group_size'))
                        except:
                            group_size = payment_data.get('group_size')
                    else:
                        group_size = payment_data.get('group_size')
                    
                    amount = float(payment.amount.value) if payment else float(payment_data['amount'])
                    
                    telegram_username = metadata.get('telegram_username')
                    group_username = metadata.get('group_username')
                    
                    # Инициализируем флаг рекуррентности в начале обработки
                    is_recurring_payment = False
                    
                    # === ОПРЕДЕЛЯЕМ payment_method_id СРАЗУ ===
                    payment_method_id = None
                    if full_payment and hasattr(full_payment, 'payment_method') and full_payment.payment_method:
                        if getattr(full_payment.payment_method, 'saved', False):
                            payment_method_id = full_payment.payment_method.id
                            logger.info(f"[YOOKASSA] Карта сохранена! payment_method_id={payment_method_id}")
                        else:
                            logger.info("[YOOKASSA] Карта НЕ сохранена — автоплатёж отключён")
                    else:
                        logger.info("[YOOKASSA] payment_method отсутствует в объекте платежа")
                    
                    # Проверяем, является ли это обновлением существующей подписки
                    upgrade_subscription_id = metadata.get('upgrade_subscription_id')
                    upgrade_from_plan = metadata.get('upgrade_from_plan')
                    
                    # Проверяем, является ли это объединенным платежом или расширением
                    is_combined = metadata.get('is_combined', 'false').lower() == 'true'
                    combine_type = metadata.get('combine_type')
                    is_expansion = metadata.get('is_expansion', 'false').lower() == 'true'
                    
                    # Проверяем, является ли это изменением периода подписки
                    is_period_upgrade = metadata.get('is_period_upgrade', 'false').lower() == 'true'
                    period_upgrade_subscription_id = metadata.get('subscription_id')  # ID подписки для обновления периода
                    period_upgrade_type = metadata.get('upgrade_type')  # 'now' или 'next'
                    
                    # Инициализируем subscription_id
                    subscription_id = None
                    
                    if is_combined and combine_type == 'pay_now':
                        # Объединенный платеж - списать сейчас
                        # Обновляем даты всех существующих подписок на сегодня
                        from moviebot.database.db_operations import get_user_personal_subscriptions, update_subscription_next_payment
                        from datetime import datetime, timedelta
                        import pytz
                        
                        existing_subs_ids = metadata.get('existing_subs_ids', '')
                        if existing_subs_ids:
                            existing_subs_ids_list = [int(x) for x in existing_subs_ids.split(',') if x.isdigit()]
                            now = datetime.now(pytz.UTC)
                            next_payment = now + timedelta(days=30)
                            
                            for sub_id in existing_subs_ids_list:
                                update_subscription_next_payment(sub_id, next_payment)
                                logger.info(f"[YOOKASSA] Обновлена дата следующего списания для подписки {sub_id} на {next_payment}")
                        
                        # Для lifetime подписок: отменяем все активные подписки и отключаем автосписания
                        if period_type == 'lifetime':
                            from moviebot.database.db_connection import get_db_connection, db_lock
                            from datetime import datetime
                            import pytz
                            
                            conn_lifetime = get_db_connection()
                            cursor_lifetime = conn_lifetime.cursor()
                            try:
                                with db_lock:
                                    if subscription_type == 'personal':
                                        cursor_lifetime.execute("""
                                            UPDATE subscriptions 
                                            SET is_active = FALSE, cancelled_at = %s, payment_method_id = NULL
                                            WHERE user_id = %s AND subscription_type = 'personal' AND is_active = TRUE
                                        """, (datetime.now(pytz.UTC), user_id))
                                    else:
                                        cursor_lifetime.execute("""
                                            UPDATE subscriptions 
                                            SET is_active = FALSE, cancelled_at = %s, payment_method_id = NULL
                                            WHERE chat_id = %s AND subscription_type = 'group' AND is_active = TRUE
                                        """, (datetime.now(pytz.UTC), chat_id))
                                    conn_lifetime.commit()
                                    cancelled_count = cursor_lifetime.rowcount
                                    if cancelled_count > 0:
                                        logger.info(f"[YOOKASSA LIFETIME] Отменено {cancelled_count} активных подписок для {subscription_type} (user_id={user_id}, chat_id={chat_id})")
                            finally:
                                try:
                                    cursor_lifetime.close()
                                except:
                                    pass
                                try:
                                    conn_lifetime.close()
                                except:
                                    pass
                        
                        # Для lifetime подписок payment_method_id должен быть NULL (отключаем автосписания)
                        final_payment_method_id = None if period_type == 'lifetime' else payment_method_id
                        
                        # Создаем новую подписку
                        try:
                            subscription_id = create_subscription(
                                chat_id=chat_id,
                                user_id=user_id,
                                subscription_type=subscription_type,
                                plan_type=plan_type,
                                period_type=period_type,
                                price=amount,
                                telegram_username=telegram_username,
                                group_username=group_username,
                                group_size=group_size,
                                payment_method_id=final_payment_method_id
                            )
                            logger.info(f"[YOOKASSA] Создана подписка: subscription_id={subscription_id}, user_id={user_id}, chat_id={chat_id}, subscription_type={subscription_type}, plan_type={plan_type}, period_type={period_type}, price={amount}₽ (объединенный платеж)")
                            
                            # Автоматически добавляем оплатившего пользователя в групповую подписку
                            if subscription_id and subscription_type == 'group':
                                try:
                                    add_subscription_member(subscription_id, user_id, telegram_username)
                                    logger.info(f"[YOOKASSA] Оплативший пользователь {user_id} (@{telegram_username}) автоматически добавлен в подписку {subscription_id}")
                                except Exception as add_error:
                                    logger.error(f"[YOOKASSA] Ошибка при автоматическом добавлении оплатившего пользователя: {add_error}", exc_info=True)
                            
                        except Exception as sub_error:
                            logger.error(f"[YOOKASSA] Ошибка при создании новой подписки: {sub_error}", exc_info=True)
                            subscription_id = None
                    elif is_combined and combine_type == 'upgrade_to_all':
                        # Переход на "Все режимы" - отменяем старые, создаем новую
                        from moviebot.database.db_operations import cancel_subscription
                        existing_subs_ids = metadata.get('existing_subs_ids', '')
                        if existing_subs_ids:
                            existing_subs_ids_list = [int(x) for x in existing_subs_ids.split(',') if x.isdigit()]
                            for sub_id in existing_subs_ids_list:
                                cancel_subscription(sub_id, user_id)
                                logger.info(f"[YOOKASSA] Отменена подписка {sub_id} при переходе на 'Все режимы'")
                        
                        # Для lifetime подписок: отменяем все активные подписки и отключаем автосписания
                        # (дополнительно к уже отмененным через existing_subs_ids)
                        if period_type == 'lifetime':
                            from moviebot.database.db_connection import get_db_connection, db_lock
                            from datetime import datetime
                            import pytz
                            
                            conn_lifetime = get_db_connection()
                            cursor_lifetime = conn_lifetime.cursor()
                            try:
                                with db_lock:
                                    # Отменяем все оставшиеся активные подписки пользователя/группы и отключаем автосписания
                                    if subscription_type == 'personal':
                                        cursor_lifetime.execute("""
                                            UPDATE subscriptions 
                                            SET is_active = FALSE, cancelled_at = %s, payment_method_id = NULL
                                            WHERE user_id = %s AND subscription_type = 'personal' AND is_active = TRUE
                                        """, (datetime.now(pytz.UTC), user_id))
                                    else:
                                        cursor_lifetime.execute("""
                                            UPDATE subscriptions 
                                            SET is_active = FALSE, cancelled_at = %s, payment_method_id = NULL
                                            WHERE chat_id = %s AND subscription_type = 'group' AND is_active = TRUE
                                        """, (datetime.now(pytz.UTC), chat_id))
                                    conn_lifetime.commit()
                                    cancelled_count = cursor_lifetime.rowcount
                                    if cancelled_count > 0:
                                        logger.info(f"[YOOKASSA LIFETIME] Дополнительно отменено {cancelled_count} активных подписок для {subscription_type} (user_id={user_id}, chat_id={chat_id})")
                            finally:
                                try:
                                    cursor_lifetime.close()
                                except:
                                    pass
                                try:
                                    conn_lifetime.close()
                                except:
                                    pass
                        
                        # Для lifetime подписок payment_method_id должен быть NULL (отключаем автосписания)
                        final_payment_method_id = None if period_type == 'lifetime' else payment_method_id
                        
                        # Создаем новую подписку "Все режимы"
                        try:
                            subscription_id = create_subscription(
                                chat_id=chat_id,
                                user_id=user_id,
                                subscription_type=subscription_type,
                                plan_type='all',
                                period_type=period_type,
                                price=amount,
                                telegram_username=telegram_username,
                                group_username=group_username,
                                group_size=group_size,
                                payment_method_id=final_payment_method_id
                            )
                            logger.info(f"[YOOKASSA] Создана подписка 'Все режимы': subscription_id={subscription_id}, user_id={user_id}, chat_id={chat_id}, subscription_type={subscription_type}, plan_type=all, period_type={period_type}, price={amount}₽")
                            
                            # Автоматически добавляем оплатившего пользователя в групповую подписку
                            if subscription_id and subscription_type == 'group':
                                try:
                                    add_subscription_member(subscription_id, user_id, telegram_username)
                                    logger.info(f"[YOOKASSA] Оплативший пользователь {user_id} (@{telegram_username}) автоматически добавлен в подписку {subscription_id}")
                                except Exception as add_error:
                                    logger.error(f"[YOOKASSA] Ошибка при автоматическом добавлении оплатившего пользователя: {add_error}", exc_info=True)
                            
                        except Exception as sub_error:
                            logger.error(f"[YOOKASSA] Ошибка при создании новой подписки: {sub_error}", exc_info=True)
                            subscription_id = None
                    elif is_expansion:
                        # Расширение подписки - обновляем размер существующей подписки
                        try:
                            expansion_sub_id = int(metadata.get('expansion_subscription_id', 0))
                            expansion_new_size = int(metadata.get('expansion_new_size', 0))
                            
                            if not expansion_sub_id or not expansion_new_size:
                                logger.error(f"[YOOKASSA] Ошибка расширения: некорректные параметры expansion_subscription_id={expansion_sub_id}, expansion_new_size={expansion_new_size}")
                                subscription_id = None
                            else:
                                from moviebot.database.db_operations import get_subscription_by_id, update_subscription_group_size
                                
                                # Получаем информацию о подписке для расширения
                                expansion_sub = get_subscription_by_id(expansion_sub_id)
                                if not expansion_sub or expansion_sub.get('user_id') != user_id:
                                    logger.error(f"[YOOKASSA] Подписка {expansion_sub_id} не найдена или не принадлежит пользователю {user_id}")
                                    subscription_id = None
                                else:
                                    # Обновляем размер подписки (цена уже рассчитана как разница)
                                    current_size = expansion_sub.get('group_size') or 2
                                    update_subscription_group_size(expansion_sub_id, expansion_new_size, amount)
                                    subscription_id = expansion_sub_id
                                    logger.info(f"[YOOKASSA] Подписка {expansion_sub_id} расширена: {current_size} -> {expansion_new_size}, доплата: {amount}₽")
                        except Exception as expansion_error:
                            logger.error(f"[YOOKASSA] Ошибка при расширении подписки: {expansion_error}", exc_info=True)
                            subscription_id = None
                    elif is_period_upgrade and period_upgrade_subscription_id:
                        # Изменение периода подписки (оплатить сейчас) - уже обработано выше
                        # subscription_id уже установлен в блоке is_period_upgrade
                        pass
                    elif upgrade_subscription_id:
                        # Обновление существующей подписки (оплата доплаты)
                        try:
                            upgrade_sub_id = int(upgrade_subscription_id)
                            from moviebot.database.db_operations import get_subscription_by_id, update_subscription_plan_type
                            
                            # Получаем информацию о подписке для обновления
                            upgrade_sub = get_subscription_by_id(upgrade_sub_id)
                            if not upgrade_sub or upgrade_sub.get('user_id') != user_id:
                                logger.error(f"[YOOKASSA] Подписка {upgrade_sub_id} не найдена или не принадлежит пользователю {user_id}")
                                subscription_id = None
                            else:
                                # Вычисляем новую цену подписки
                                group_size_upgrade = upgrade_sub.get('group_size')
                                period_type_upgrade = upgrade_sub.get('period_type', 'month')
                                
                                # Импортируем SUBSCRIPTION_PRICES для расчета новой цены
                                from moviebot.bot.callbacks.payment_callbacks import SUBSCRIPTION_PRICES
                                
                                if subscription_type == 'personal':
                                    new_price = SUBSCRIPTION_PRICES['personal'][plan_type].get(period_type_upgrade, 0)
                                else:
                                    group_size_str = str(group_size_upgrade) if group_size_upgrade else '2'
                                    new_price = SUBSCRIPTION_PRICES['group'][group_size_str][plan_type].get(period_type_upgrade, 0)
                                
                                # Обновляем подписку: меняем plan_type и price
                                update_subscription_plan_type(upgrade_sub_id, plan_type, new_price)
                                
                                # payment_method_id будет обновлен позже, если карта сохранена
                                
                                subscription_id = upgrade_sub_id
                                logger.info(f"[YOOKASSA] Подписка {upgrade_sub_id} обновлена: {upgrade_from_plan} -> {plan_type}, цена: {new_price}₽")
                                
                        except Exception as upgrade_error:
                            logger.error(f"[YOOKASSA] Ошибка при обновлении подписки: {upgrade_error}", exc_info=True)
                            subscription_id = None
                    elif is_period_upgrade and period_upgrade_subscription_id:
                        # Изменение периода подписки (оплатить сейчас)
                        try:
                            period_sub_id = int(period_upgrade_subscription_id)
                            from moviebot.database.db_operations import get_subscription_by_id
                            from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
                            from datetime import datetime, timedelta
                            from dateutil.relativedelta import relativedelta
                            import pytz
                            
                            # Получаем информацию о подписке
                            period_sub = get_subscription_by_id(period_sub_id)
                            if not period_sub or period_sub.get('user_id') != user_id:
                                logger.error(f"[YOOKASSA] Подписка {period_sub_id} не найдена или не принадлежит пользователю {user_id}")
                                subscription_id = None
                            else:
                                # Обновляем период и цену подписки
                                # amount - это доплата, нужно обновить на полную цену новой подписки
                                new_period_type = period_type  # Новый период из metadata
                                new_full_price = float(metadata.get('new_price', amount))  # Полная цена новой подписки
                                
                                conn_period = get_db_connection()
                                cursor_period = get_db_cursor()
                                try:
                                    with db_lock:
                                        # Обновляем period_type, price и сдвигаем даты на сегодня
                                        now = datetime.now(pytz.UTC)
                                        
                                        # Вычисляем новую дату следующего платежа в зависимости от периода
                                        if new_period_type == 'month':
                                            next_payment = now + relativedelta(months=1)
                                            expires_at = next_payment
                                        elif new_period_type == '3months':
                                            next_payment = now + relativedelta(months=3)
                                            expires_at = next_payment
                                        elif new_period_type == 'year':
                                            next_payment = now + relativedelta(years=1)
                                            expires_at = next_payment
                                        elif new_period_type == 'lifetime':
                                            next_payment = None
                                            expires_at = None
                                        else:
                                            next_payment = now + timedelta(days=30)
                                            expires_at = next_payment
                                        
                                        cursor_period.execute("""
                                            UPDATE subscriptions 
                                            SET period_type = %s, price = %s, 
                                                next_payment_date = %s, expires_at = %s,
                                                activated_at = %s
                                            WHERE id = %s
                                        """, (new_period_type, new_full_price, next_payment, expires_at, now, period_sub_id))
                                        conn_period.commit()
                                        
                                        logger.info(f"[YOOKASSA PERIOD UPGRADE] Обновлена подписка {period_sub_id}: period_type={new_period_type}, price={new_full_price}₽, next_payment={next_payment}")
                                        
                                        subscription_id = period_sub_id
                                finally:
                                    try:
                                        cursor_period.close()
                                    except:
                                        pass
                                    try:
                                        conn_period.close()
                                    except:
                                        pass
                        except Exception as period_error:
                            logger.error(f"[YOOKASSA] Ошибка при обновлении периода подписки: {period_error}", exc_info=True)
                            subscription_id = None
                    else:
                        # Обычная логика (без объединения и без обновления)
                        # Проверяем, есть ли уже активная подписка с такими же параметрами
                        from moviebot.database.db_operations import get_active_subscription, renew_subscription
                        existing_sub = get_active_subscription(chat_id, user_id, subscription_type)
                        
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
                                logger.info(f"[YOOKASSA] Подписка продлена: subscription_id={subscription_id}, user_id={user_id}, chat_id={chat_id}, subscription_type={subscription_type}, plan_type={existing_plan}, period_type={period_type}")
                                
                                # payment_method_id будет обновлен позже, если карта сохранена
                                
                                # Помечаем как рекуррентный платеж для уведомления
                                is_recurring_payment = True
                                
                            else:
                                # Параметры не совпадают - создаем новую подписку
                                # Для lifetime подписок: отменяем все активные подписки пользователя/группы и отключаем автосписания
                                if period_type == 'lifetime':
                                    from moviebot.database.db_connection import get_db_connection, db_lock
                                    from datetime import datetime
                                    import pytz
                                    
                                    conn_lifetime = get_db_connection()
                                    cursor_lifetime = conn_lifetime.cursor()
                                    try:
                                        with db_lock:
                                            # Отменяем все активные подписки пользователя/группы и отключаем автосписания
                                            if subscription_type == 'personal':
                                                # Для личных подписок отменяем все личные подписки пользователя
                                                cursor_lifetime.execute("""
                                                    UPDATE subscriptions 
                                                    SET is_active = FALSE, cancelled_at = %s, payment_method_id = NULL
                                                    WHERE user_id = %s AND subscription_type = 'personal' AND is_active = TRUE
                                                """, (datetime.now(pytz.UTC), user_id))
                                            else:
                                                # Для групповых подписок отменяем все групповые подписки группы
                                                cursor_lifetime.execute("""
                                                    UPDATE subscriptions 
                                                    SET is_active = FALSE, cancelled_at = %s, payment_method_id = NULL
                                                    WHERE chat_id = %s AND subscription_type = 'group' AND is_active = TRUE
                                                """, (datetime.now(pytz.UTC), chat_id))
                                            conn_lifetime.commit()
                                            cancelled_count = cursor_lifetime.rowcount
                                            logger.info(f"[YOOKASSA LIFETIME] Отменено {cancelled_count} активных подписок для {subscription_type} (user_id={user_id}, chat_id={chat_id})")
                                    finally:
                                        try:
                                            cursor_lifetime.close()
                                        except:
                                            pass
                                        try:
                                            conn_lifetime.close()
                                        except:
                                            pass
                                
                                # Для lifetime подписок payment_method_id должен быть NULL (отключаем автосписания)
                                final_payment_method_id = None if period_type == 'lifetime' else payment_method_id
                                
                                # Проверяем, был ли промокод только на первый месяц
                                # Если да, сохраняем discounted_price в подписку, но для рекуррентных платежей будем использовать полную стоимость
                                is_first_month_promo = metadata.get('is_first_month_promo', 'false').lower() == 'true'
                                subscription_price = amount  # Сохраняем фактически оплаченную сумму (со скидкой, если был промокод)
                                
                                try:
                                    subscription_id = create_subscription(
                                        chat_id=chat_id,
                                        user_id=user_id,
                                        subscription_type=subscription_type,
                                        plan_type=plan_type,
                                        period_type=period_type,
                                        price=subscription_price,  # Сохраняем discounted_price в подписку
                                        telegram_username=telegram_username,
                                        group_username=group_username,
                                        group_size=group_size,
                                        payment_method_id=final_payment_method_id
                                    )
                                    logger.info(f"[YOOKASSA] Создана подписка: subscription_id={subscription_id}, user_id={user_id}, chat_id={chat_id}, subscription_type={subscription_type}, plan_type={plan_type}, period_type={period_type}, price={subscription_price}₽ (is_first_month_promo={is_first_month_promo})")
                                    
                                    # Автоматически добавляем оплатившего пользователя в групповую подписку
                                    if subscription_id and subscription_type == 'group':
                                        try:
                                            add_subscription_member(subscription_id, user_id, telegram_username)
                                            logger.info(f"[YOOKASSA] Оплативший пользователь {user_id} (@{telegram_username}) автоматически добавлен в подписку {subscription_id}")
                                        except Exception as add_error:
                                            logger.error(f"[YOOKASSA] Ошибка при автоматическом добавлении оплатившего пользователя: {add_error}", exc_info=True)
                                    
                                except Exception as sub_error:
                                    logger.error(f"[YOOKASSA] Ошибка при создании новой подписки: {sub_error}", exc_info=True)
                                    subscription_id = None
                        else:
                            # Нет активной подписки - создаем новую
                            # Для lifetime подписок: отменяем все активные подписки пользователя/группы и отключаем автосписания
                            if period_type == 'lifetime':
                                from moviebot.database.db_connection import get_db_connection, db_lock
                                from datetime import datetime
                                import pytz
                                
                                conn_lifetime = get_db_connection()
                                cursor_lifetime = conn_lifetime.cursor()
                                try:
                                    with db_lock:
                                        # Отменяем все активные подписки пользователя/группы и отключаем автосписания
                                        if subscription_type == 'personal':
                                            # Для личных подписок отменяем все личные подписки пользователя
                                            cursor_lifetime.execute("""
                                                UPDATE subscriptions 
                                                SET is_active = FALSE, cancelled_at = %s, payment_method_id = NULL
                                                WHERE user_id = %s AND subscription_type = 'personal' AND is_active = TRUE
                                            """, (datetime.now(pytz.UTC), user_id))
                                        else:
                                            # Для групповых подписок отменяем все групповые подписки группы
                                            cursor_lifetime.execute("""
                                                UPDATE subscriptions 
                                                SET is_active = FALSE, cancelled_at = %s, payment_method_id = NULL
                                                WHERE chat_id = %s AND subscription_type = 'group' AND is_active = TRUE
                                            """, (datetime.now(pytz.UTC), chat_id))
                                        conn_lifetime.commit()
                                        cancelled_count = cursor_lifetime.rowcount
                                        logger.info(f"[YOOKASSA LIFETIME] Отменено {cancelled_count} активных подписок для {subscription_type} (user_id={user_id}, chat_id={chat_id})")
                                finally:
                                    try:
                                        cursor_lifetime.close()
                                    except:
                                        pass
                                    try:
                                        conn_lifetime.close()
                                    except:
                                        pass
                            
                            # Для lifetime подписок payment_method_id должен быть NULL (отключаем автосписания)
                            final_payment_method_id = None if period_type == 'lifetime' else payment_method_id
                            
                            try:
                                subscription_id = create_subscription(
                                    chat_id=chat_id,
                                    user_id=user_id,
                                    subscription_type=subscription_type,
                                    plan_type=plan_type,
                                    period_type=period_type,
                                    price=amount,
                                    telegram_username=telegram_username,
                                    group_username=group_username,
                                    group_size=group_size,
                                    payment_method_id=final_payment_method_id
                                )
                                logger.info(f"[YOOKASSA] Создана подписка: subscription_id={subscription_id}, user_id={user_id}, chat_id={chat_id}, subscription_type={subscription_type}, plan_type={plan_type}, period_type={period_type}, price={amount}₽")
                                
                                # Автоматически добавляем оплатившего пользователя в групповую подписку
                                if subscription_id and subscription_type == 'group':
                                    try:
                                        add_subscription_member(subscription_id, user_id, telegram_username)
                                        logger.info(f"[YOOKASSA] Оплативший пользователь {user_id} (@{telegram_username}) автоматически добавлен в подписку {subscription_id}")
                                    except Exception as add_error:
                                        logger.error(f"[YOOKASSA] Ошибка при автоматическом добавлении оплатившего пользователя: {add_error}", exc_info=True)
                                
                            except Exception as sub_error:
                                logger.error(f"[YOOKASSA] Ошибка при создании новой подписки: {sub_error}", exc_info=True)
                                subscription_id = None
                    
                    # === СОХРАНЕНИЕ payment_method_id В ПЛАТЕЖ ===
                    if payment_method_id:
                        from moviebot.database.db_connection import get_db_connection, db_lock
                        conn_payment = get_db_connection()
                        cursor_payment = conn_payment.cursor()
                        with db_lock:
                            cursor_payment.execute("""
                                UPDATE payments 
                                SET payment_method_id = %s, updated_at = NOW()
                                WHERE payment_id = %s
                            """, (payment_method_id, payment_data['payment_id']))
                            conn_payment.commit()
                        logger.info(f"[YOOKASSA] payment_method_id {payment_method_id} сохранён в платеж")
                    
                    # === СОХРАНЕНИЕ payment_method_id В ПОДПИСКУ ТОЛЬКО ЕСЛИ saved: true ===
                    if full_payment and hasattr(full_payment, 'payment_method') and full_payment.payment_method:
                        if getattr(full_payment.payment_method, 'saved', False) and subscription_id:
                            saved_pm_id = full_payment.payment_method.id
                            from moviebot.database.db_connection import get_db_connection, db_lock
                            conn_sub = get_db_connection()
                            cursor_sub = conn_sub.cursor()
                            with db_lock:
                                cursor_sub.execute("""
                                    UPDATE subscriptions 
                                    SET payment_method_id = %s
                                    WHERE id = %s
                                """, (saved_pm_id, subscription_id))
                                conn_sub.commit()
                            logger.info(f"[YOOKASSA] Автоплатёж включён для подписки {subscription_id} (payment_method_id={saved_pm_id})")
                    
                    # Создаем чек от самозанятого
                    check_url = None
                    pdf_url = None
                    logger.info(f"[YOOKASSA CHECK] ===== НАЧАЛО СОЗДАНИЯ ЧЕКА =====")
                    logger.info(f"[YOOKASSA CHECK] user_id={user_id}, chat_id={chat_id}, amount={amount}, subscription_type={subscription_type}, plan_type={plan_type}")
                    try:
                        from moviebot.services.nalog_service import create_check
                        import os
                        
                        # Проверяем наличие настроек для чека
                        nalog_inn = os.getenv('NALOG_INN')
                        nalog_password = os.getenv('NALOG_PASSWORD')
                        
                        # Детальное логирование для отладки
                        logger.info(f"[YOOKASSA CHECK] Проверка переменных окружения:")
                        logger.info(f"[YOOKASSA CHECK] NALOG_INN присутствует: {nalog_inn is not None}, значение: {'***' if nalog_inn else 'None'}")
                        logger.info(f"[YOOKASSA CHECK] NALOG_PASSWORD присутствует: {nalog_password is not None}, значение: {'***' if nalog_password else 'None'}")
                        
                        # Проверяем, что значения не пустые (после strip)
                        if nalog_inn:
                            nalog_inn = nalog_inn.strip()
                        if nalog_password:
                            nalog_password = nalog_password.strip()
                        
                        if not nalog_inn or not nalog_password:
                            logger.warning(f"[YOOKASSA CHECK] ⚠️ NALOG_INN или NALOG_PASSWORD не настроены!")
                            logger.warning(f"[YOOKASSA CHECK] NALOG_INN: {'установлен (пусто после strip)' if nalog_inn is not None and not nalog_inn else 'НЕ УСТАНОВЛЕН'}")
                            logger.warning(f"[YOOKASSA CHECK] NALOG_PASSWORD: {'установлен (пусто после strip)' if nalog_password is not None and not nalog_password else 'НЕ УСТАНОВЛЕН'}")
                            logger.warning(f"[YOOKASSA CHECK] Чек не будет создан из-за отсутствия настроек")
                        else:
                            logger.info(f"[YOOKASSA CHECK] ✅ Настройки NALOG найдены, продолжаем создание чека")
                            
                            # Формируем описание подписки
                            subscription_type_name = 'Личная подписка' if subscription_type == 'personal' else 'Групповая подписка'
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
                                'all': '💎 Movie Planner PRO'
                            }
                            plan_name = plan_names.get(plan_type, plan_type)
                            
                            description = f"{subscription_type_name}: {plan_name}, период: {period_name}"
                            
                            # Получаем имя пользователя из metadata или БД
                            user_name = metadata.get('telegram_username')
                            if not user_name:
                                # Пытаемся получить из БД или используем дефолтное
                                user_name = f"user_{user_id}"
                            
                            logger.info(f"[YOOKASSA CHECK] Параметры чека: amount={amount}, description={description}, user_name={user_name}")
                            logger.info(f"[YOOKASSA CHECK] Вызываем create_check...")
                            check_url, pdf_url = create_check(
                                amount_rub=float(amount),
                                description=description,
                                user_name=user_name
                            )
                            
                            logger.info(f"[YOOKASSA CHECK] Результат create_check: check_url={check_url}, pdf_url={pdf_url}")
                            
                            if check_url:
                                logger.info(f"[YOOKASSA CHECK] ✅✅✅ ЧЕК УСПЕШНО СОЗДАН! ✅✅✅")
                                logger.info(f"[YOOKASSA CHECK] check_url={check_url}")
                                if pdf_url:
                                    logger.info(f"[YOOKASSA CHECK] pdf_url={pdf_url}")
                            else:
                                logger.warning(f"[YOOKASSA CHECK] ⚠️ create_check вернул check_url=None (чек не создан)")
                                logger.warning(f"[YOOKASSA CHECK] Возможные причины: ошибка API nalog.ru, неверные настройки, или другая проблема")
                    except Exception as check_error:
                        logger.error(f"[YOOKASSA CHECK] ❌❌❌ ИСКЛЮЧЕНИЕ ПРИ СОЗДАНИИ ЧЕКА! ❌❌❌")
                        logger.error(f"[YOOKASSA CHECK] Тип ошибки: {type(check_error).__name__}")
                        logger.error(f"[YOOKASSA CHECK] Сообщение: {str(check_error)}")
                        logger.error(f"[YOOKASSA CHECK] Traceback:", exc_info=True)
                        # Продолжаем выполнение даже если чек не создан
                    
                    logger.info(f"[YOOKASSA CHECK] ===== ЗАВЕРШЕНИЕ СОЗДАНИЯ ЧЕКА =====")
                    logger.info(f"[YOOKASSA CHECK] Итоговый результат: check_url={check_url}, pdf_url={pdf_url}")
                    logger.info(f"[YOOKASSA CHECK] Будет ли чек добавлен в сообщение: {'ДА' if check_url else 'НЕТ'}")
                
                # === ОТПРАВЛЯЕМ УВЕДОМЛЕНИЕ ТОЛЬКО ОДИН РАЗ ===
                if subscription_id:
                    from moviebot.scheduler import send_successful_payment_notification
                    # Используем флаг рекуррентности, определенный выше
                    send_successful_payment_notification(
                        chat_id=chat_id,
                        subscription_id=subscription_id,
                        subscription_type=subscription_type,
                        plan_type=plan_type,
                        period_type=period_type,
                        is_recurring=is_recurring_payment,
                        check_url=check_url,
                        pdf_url=pdf_url
                    )
                    logger.info(f"[YOOKASSA] Уведомление отправлено один раз для подписки {subscription_id}, is_recurring={is_recurring_payment}")


                            
                elif payment_status == 'succeeded' and db_status == 'succeeded':
                    # Платеж уже обработан, проверяем, есть ли подписка
                    logger.info(f"[YOOKASSA] Платеж уже обработан (статус: {db_status}), проверяем наличие подписки")
                    
                    # Инициализируем check_url и pdf_url (могут быть None, если чек не создан)
                    check_url = None
                    pdf_url = None
                    
                    subscription_id_from_payment = payment_data.get('subscription_id')
                    if not subscription_id_from_payment:
                        logger.warning(f"[YOOKASSA] Платеж обработан, но subscription_id отсутствует. Создаем подписку и отправляем сообщение.")
                        
                        # Получаем данные из metadata или payment_data
                        if payment and hasattr(payment, 'metadata') and payment.metadata:
                            metadata = payment.metadata
                        elif event_json.get('object', {}).get('metadata'):
                            metadata = event_json.get('object', {}).get('metadata', {})
                        else:
                            metadata = {}
                        
                        user_id = int(metadata.get('user_id', payment_data['user_id']))
                        chat_id = int(metadata.get('chat_id', payment_data['chat_id']))
                        subscription_type = metadata.get('subscription_type', payment_data['subscription_type'])
                        plan_type = metadata.get('plan_type', payment_data['plan_type'])
                        period_type = metadata.get('period_type', payment_data['period_type'])
                        
                        # Обрабатываем group_size
                        group_size = None
                        if metadata.get('group_size'):
                            try:
                                group_size = int(metadata.get('group_size'))
                            except:
                                group_size = payment_data.get('group_size')
                        else:
                            group_size = payment_data.get('group_size')
                        
                        # Получаем сумму из платежа или из БД
                        if payment:
                            amount = float(payment.amount.value)
                            payment_method_id = None
                            if hasattr(payment, 'payment_method') and payment.payment_method:
                                if hasattr(payment.payment_method, 'id'):
                                    payment_method_id = payment.payment_method.id
                                elif hasattr(payment.payment_method, 'saved'):
                                    payment_method_id = getattr(payment.payment_method, 'id', None)
                        else:
                            amount = float(payment_data['amount'])
                            payment_method_id = None
                        
                        # Определяем telegram_username и group_username из metadata
                        telegram_username = metadata.get('telegram_username')
                        group_username = metadata.get('group_username')
                        
                        # Для lifetime подписок: отменяем все активные подписки и отключаем автосписания
                        if period_type == 'lifetime':
                            from moviebot.database.db_connection import get_db_connection, db_lock
                            from datetime import datetime
                            import pytz
                            
                            conn_lifetime = get_db_connection()
                            cursor_lifetime = conn_lifetime.cursor()
                            try:
                                with db_lock:
                                    if subscription_type == 'personal':
                                        cursor_lifetime.execute("""
                                            UPDATE subscriptions 
                                            SET is_active = FALSE, cancelled_at = %s, payment_method_id = NULL
                                            WHERE user_id = %s AND subscription_type = 'personal' AND is_active = TRUE
                                        """, (datetime.now(pytz.UTC), user_id))
                                    else:
                                        cursor_lifetime.execute("""
                                            UPDATE subscriptions 
                                            SET is_active = FALSE, cancelled_at = %s, payment_method_id = NULL
                                            WHERE chat_id = %s AND subscription_type = 'group' AND is_active = TRUE
                                        """, (datetime.now(pytz.UTC), chat_id))
                                    conn_lifetime.commit()
                                    cancelled_count = cursor_lifetime.rowcount
                                    if cancelled_count > 0:
                                        logger.info(f"[YOOKASSA LIFETIME] Отменено {cancelled_count} активных подписок для {subscription_type} (user_id={user_id}, chat_id={chat_id})")
                            finally:
                                try:
                                    cursor_lifetime.close()
                                except:
                                    pass
                                try:
                                    conn_lifetime.close()
                                except:
                                    pass
                        
                        # Для lifetime подписок payment_method_id должен быть NULL (отключаем автосписания)
                        final_payment_method_id = None if period_type == 'lifetime' else payment_method_id
                        
                        # Создаем подписку
                        subscription_id = None
                        try:
                            subscription_id = create_subscription(
                                chat_id=chat_id,
                                user_id=user_id,
                                subscription_type=subscription_type,
                                plan_type=plan_type,
                                period_type=period_type,
                                price=amount,
                                telegram_username=telegram_username,
                                group_username=group_username,
                                group_size=group_size,
                                payment_method_id=final_payment_method_id
                            )
                            logger.info(f"[YOOKASSA] Создана подписка для уже обработанного платежа: subscription_id={subscription_id}, user_id={user_id}, chat_id={chat_id}, subscription_type={subscription_type}, plan_type={plan_type}, period_type={period_type}, price={amount}₽")
                            
                            # Обновляем платеж с subscription_id
                            update_payment_status(payment_data['payment_id'], 'succeeded', subscription_id)
                        except Exception as sub_error:
                            logger.error(f"[YOOKASSA] Ошибка при создании подписки: {sub_error}", exc_info=True)
                            # Все равно обновляем статус платежа
                            try:
                                update_payment_status(payment_data['payment_id'], 'succeeded', None)
                            except:
                                pass
                        
                        # Отправляем сообщение с благодарностью (всегда, даже если подписка не создана)
                        try:
                            from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                            
                            target_chat_id = chat_id
                            
                            # Формируем описание функций в зависимости от типа подписки
                            if subscription_type == 'personal':
                                # Определяем название тарифа
                                plan_names = {
                                    'notifications': 'Уведомления о сериалах',
                                    'recommendations': 'Рекомендации',
                                    'tickets': 'Билеты',
                                    'all': '💎 Movie Planner PRO'
                                }
                                tariff_name = plan_names.get(plan_type, plan_type)
                                
                                text = "Спасибо за покупку! 🎉\n\n"
                                text += f"Ваша новая подписка: <b>{tariff_name}</b>\n\n"
                                text += "Входит в подписку:\n"
                                if plan_type == 'notifications':
                                    text += "📺 Уведомления о сериалах — новые серии, настройка времени, прогресс сезонов\n"
                                elif plan_type == 'recommendations':
                                    text += "🎯 Рекомендации — по базе, по Кинопоиску, импорт базы\n"
                                elif plan_type == 'tickets':
                                    text += "🎟 Билеты — добавление билетов, напоминания перед мероприятием\n"
                                else:  # all
                                    text += "📺 Трекер сериалов — серии, сезоны, уведомления о новых сериях\n"
                                    text += "🎟 Билеты и напоминания — добавление билетов, уведомления перед сеансом\n"
                                    text += "🎯 Рекомендации — по базе, по Кинопоиску, импорт базы\n"
                                
                                text += "\nПриятного просмотра!"
                                
                                bot.send_message(target_chat_id, text, parse_mode='HTML')
                                logger.info(f"[YOOKASSA] ✅ Сообщение отправлено для пользователя {user_id}, subscription_id {subscription_id}")
                                
                            elif subscription_type == 'group':
                                from moviebot.database.db_operations import get_active_group_users, get_subscription_members
                                from moviebot.bot.bot_init import BOT_ID
                                
                                members_dict = get_subscription_members(subscription_id) if subscription_id else {}
                                members_count = len(members_dict) if members_dict else 0
                                active_users = get_active_group_users(chat_id, bot_id=BOT_ID)
                                active_count = len(active_users) if active_users else 0
                                
                                # Формируем описание возможностей (тезисно: сериалы, билеты, рекомендации)
                                features_text = ""
                                if plan_type == 'all':
                                    features_text = "💎 <b>Movie Planner PRO</b>\n\n"
                                    features_text += "📺 Трекер сериалов — серии, сезоны, уведомления о новых сериях\n"
                                    features_text += "🎟 Билеты и напоминания — добавление билетов, уведомления перед сеансом\n"
                                    features_text += "🎯 Рекомендации — по базе, по Кинопоиску, импорт базы\n"
                                elif plan_type == 'notifications':
                                    features_text = "📺 <b>Уведомления о сериалах</b>\n"
                                    features_text += "• Новые серии, настройка времени, прогресс сезонов\n"
                                elif plan_type == 'recommendations':
                                    features_text = "🎯 <b>Рекомендации</b>\n"
                                    features_text += "• По базе, по Кинопоиску, импорт базы\n"
                                elif plan_type == 'tickets':
                                    features_text = "🎟 <b>Билеты</b>\n"
                                    features_text += "• Добавление билетов, напоминания перед мероприятием\n"
                                
                                # Определяем название тарифа для группы
                                plan_names = {
                                    'notifications': 'Уведомления о сериалах',
                                    'recommendations': 'Рекомендации',
                                    'tickets': 'Билеты',
                                    'all': '💎 Movie Planner PRO'
                                }
                                tariff_name = plan_names.get(plan_type, plan_type)
                                
                                group_text = "Спасибо за покупку! 🎉\n\n"
                                group_text += f"Ваша новая подписка: <b>{tariff_name}</b>\n\n"
                                group_text += "Вот какой функционал вам теперь доступен:\n\n"
                                group_text += features_text
                                
                                if group_size:
                                    group_text += f"\n\n👥 Участников в подписке: <b>{members_count if members_count > 0 else active_count}</b> из {group_size}"
                                
                                group_text += "\n"
                                
                                # Добавляем информацию о чеке, если он был создан
                                if check_url:
                                    group_text += f"📄 <b>Чек от самозанятого:</b>\n"
                                    group_text += f"{check_url}\n"
                                    if pdf_url:
                                        group_text += f"\n📥 <a href=\"{pdf_url}\">Скачать PDF</a>\n"
                                
                                group_text += "\nПриятного просмотра!"
                                
                                # Для групповых подписок отправляем только в группу, не в личку
                                bot.send_message(chat_id, group_text, parse_mode='HTML')
                                logger.info(f"[YOOKASSA] ✅ Сообщение отправлено в группу {chat_id} для user_id {user_id}, subscription_id {subscription_id}")
                        except Exception as e:
                            logger.error(f"[YOOKASSA] Ошибка отправки сообщения для уже обработанного платежа: {e}", exc_info=True)
                    else:
                        logger.info(f"[YOOKASSA] Платеж обработан, подписка {subscription_id_from_payment} существует")
                        
                        # Отправляем сообщение с благодарностью, если подписка существует
                        try:
                            from moviebot.database.db_operations import get_subscription_by_id
                            sub = get_subscription_by_id(subscription_id_from_payment)
                            
                            # Если подписка не найдена, используем данные из payment_data
                            if not sub:
                                logger.warning(f"[YOOKASSA] Подписка {subscription_id_from_payment} не найдена, используем данные из payment_data")
                                # Получаем данные из metadata или payment_data
                                if payment and hasattr(payment, 'metadata') and payment.metadata:
                                    metadata = payment.metadata
                                elif event_json.get('object', {}).get('metadata'):
                                    metadata = event_json.get('object', {}).get('metadata', {})
                                else:
                                    metadata = {}
                                
                                sub = {
                                    'user_id': int(metadata.get('user_id', payment_data['user_id'])),
                                    'chat_id': int(metadata.get('chat_id', payment_data['chat_id'])),
                                    'subscription_type': metadata.get('subscription_type', payment_data['subscription_type']),
                                    'plan_type': metadata.get('plan_type', payment_data['plan_type']),
                                    'group_size': payment_data.get('group_size')
                                }
                            
                            if sub:
                                user_id = sub.get('user_id')
                                chat_id = sub.get('chat_id')
                                subscription_type = sub.get('subscription_type')
                                plan_type = sub.get('plan_type')
                                
                                from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                                
                                # Формируем описание функций
                                if subscription_type == 'personal':
                                    text = "Спасибо за подписку! Вот какой функционал вам теперь доступен:\n\n"
                                    text += "👤 <b>Личная подписка активирована</b>\n\n"
                                    
                                    if plan_type == 'notifications':
                                        text += "🔔 <b>Доступные функции:</b>\n"
                                        text += "• Автоматические уведомления о выходе новых серий\n"
                                        text += "• Настройка времени уведомлений (будни/выходные)\n"
                                        text += "• Персонализированные напоминания для каждого сериала\n"
                                        text += "• Отслеживание прогресса просмотра сезонов\n"
                                    elif plan_type == 'recommendations':
                                        text += "🎯 <b>Доступные функции:</b>\n"
                                        text += "• Режим \"По оценкам в базе\" — рекомендации по оценкам фильмов, добавленных в базу чата или группы\n"
                                        text += "• Режим \"Рандом по Кинопоиску\" — случайный фильм из Кинопоиска по фильтрам\n"
                                        text += "• Режим рандомайзера \"По моим оценкам\" — рекомендации по оценкам из Кинопоиска\n"
                                        text += "• Импорт базы из Кинопоиска\n"
                                    elif plan_type == 'tickets':
                                        text += "🎫 <b>Доступные функции:</b>\n"
                                        text += "• Добавление билетов на сеансы и мероприятия\n"
                                        text += "• Настраиваемые уведомления с билетами перед мероприятием\n"
                                    else:  # all
                                        text += "💎 <b>Movie Planner PRO</b>\n\n"
                                        text += "📺 Трекер сериалов — серии, сезоны, уведомления о новых сериях\n"
                                        text += "🎟 Билеты и напоминания — добавление билетов, уведомления перед сеансом\n"
                                        text += "🎯 Рекомендации — по базе, по Кинопоиску, импорт базы\n"
                                    
                                    bot.send_message(chat_id, text, parse_mode='HTML')
                                    logger.info(f"[YOOKASSA] ✅ Сообщение отправлено для пользователя {user_id}, subscription_id {subscription_id_from_payment}")
                                
                                elif subscription_type == 'group':
                                    from moviebot.database.db_operations import get_active_group_users, get_subscription_members
                                    from moviebot.bot.bot_init import BOT_ID
                                    
                                    members_dict = get_subscription_members(subscription_id_from_payment) if subscription_id_from_payment else {}
                                    members_count = len(members_dict) if members_dict else 0
                                    active_users = get_active_group_users(chat_id, bot_id=BOT_ID)
                                    active_count = len(active_users) if active_users else 0
                                    group_size = sub.get('group_size')
                                    
                                    # Формируем описание возможностей (тезисно: сериалы, билеты, рекомендации)
                                    features_text = ""
                                    if plan_type == 'all':
                                        features_text = "💎 <b>Movie Planner PRO</b>\n\n"
                                        features_text += "📺 Трекер сериалов — серии, сезоны, уведомления о новых сериях\n"
                                        features_text += "🎟 Билеты и напоминания — добавление билетов, уведомления перед сеансом\n"
                                        features_text += "🎯 Рекомендации — по базе, по Кинопоиску, импорт базы\n"
                                    elif plan_type == 'notifications':
                                        features_text = "📺 <b>Уведомления о сериалах</b>\n"
                                        features_text += "• Новые серии, настройка времени, прогресс сезонов\n"
                                    elif plan_type == 'recommendations':
                                        features_text = "🎯 <b>Рекомендации</b>\n"
                                        features_text += "• По базе, по Кинопоиску, импорт базы\n"
                                    elif plan_type == 'tickets':
                                        features_text = "🎟 <b>Билеты</b>\n"
                                        features_text += "• Добавление билетов, напоминания перед мероприятием\n"
                                    
                                    group_text = "Спасибо за подписку! Вот какой функционал вам теперь доступен:\n\n"
                                    group_text += "👥 <b>Групповая подписка активирована</b>\n\n"
                                    group_text += features_text
                                    
                                    if group_size:
                                        group_text += f"\n\n👥 Участников в подписке: <b>{members_count if members_count > 0 else active_count}</b> из {group_size}"
                                    
                                    group_text += "\n"
                                    
                                    # Добавляем информацию о чеке, если он был создан
                                    if check_url:
                                        group_text += f"\n📄 <b>Чек от самозанятого:</b>\n"
                                        group_text += f"{check_url}\n"
                                        if pdf_url:
                                            group_text += f"\n📥 <a href=\"{pdf_url}\">Скачать PDF</a>\n"
                                    
                                    group_text += "\nСпасибо за покупку! 🎉"
                                    
                                    bot.send_message(chat_id, group_text, parse_mode='HTML')
                                    
                                    # Отправляем в личку
                                    private_text = "Спасибо за подписку! Вот какой функционал вам теперь доступен:\n\n"
                                    private_text += "👥 <b>Групповая подписка активирована</b>\n\n"
                                    private_text += features_text
                                    
                                    if group_size:
                                        private_text += f"\n\n👥 Участников в подписке: <b>{members_count if members_count > 0 else active_count}</b> из {group_size}"
                                    
                                    private_text += "\n\nСпасибо за покупку! 🎉"
                                    
                                    bot.send_message(user_id, private_text, parse_mode='HTML')
                                    logger.info(f"[YOOKASSA] ✅ Сообщения отправлены для группы {chat_id}, user_id {user_id}, subscription_id {subscription_id_from_payment}")
                        except Exception as e:
                            logger.error(f"[YOOKASSA] Ошибка отправки сообщения для существующей подписки: {e}", exc_info=True)
                else:
                    logger.warning(f"[YOOKASSA] Событие payment.succeeded, но статус платежа не succeeded: {payment_status} (статус в БД: {db_status})")
            elif event_json.get('event') == 'payment.canceled':
                # Обработка отмены платежа
                payment_id = event_json.get('object', {}).get('id')
                if payment_id:
                    logger.info(f"[YOOKASSA] Платеж отменен: {payment_id}")
                    from moviebot.database.db_operations import get_payment_by_yookassa_id, update_payment_status
                    payment_data = get_payment_by_yookassa_id(payment_id)
                    if payment_data:
                        update_payment_status(payment_data['payment_id'], 'canceled')
                        logger.info(f"[YOOKASSA] Статус платежа {payment_data['payment_id']} обновлен на 'canceled'")
                    else:
                        logger.warning(f"[YOOKASSA] Платеж {payment_id} не найден в БД")
                else:
                    logger.warning(f"[YOOKASSA] Платеж отменен, но payment_id отсутствует")
            else:
                # Для других событий (например, если платеж уже обработан)
                if payment_data:
                    logger.warning(f"[YOOKASSA] Платеж уже обработан ранее (статус: {payment_data.get('status')})")
                else:
                    logger.info(f"[YOOKASSA] Неизвестное событие: {event_json.get('event')}")
            
            logger.info(f"[YOOKASSA] Обработка завершена, возвращаем успешный ответ")
            return jsonify({'status': 'ok'}), 200
            
        except Exception as e:
            logger.error(f"[YOOKASSA] Ошибка обработки webhook: {e}", exc_info=True)
            return jsonify({'error': str(e)}), 500
    
    @app.route('/', methods=['GET'])
    def root():
        logger.info("[ROOT] Root запрос получен")
        return jsonify({'status': 'ok', 'service': 'moviebot'}), 200

    # ← Здесь был старый health — УДАЛИТЬ полностью (включая try... весь блок ниже)

    @app.route('/health', methods=['GET'])
    def health():
        """Улучшенный health check endpoint с проверкой всех компонентов"""
        logger.info("[HEALTH] Health check запрос получен")
        
        try:
            # Пытаемся получить статус от watchdog, если он доступен
            try:
                import sys
                import os
                # Добавляем путь к корню проекта для импорта utils.watchdog
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                if project_root not in sys.path:
                    sys.path.insert(0, project_root)
                from utils.watchdog import get_watchdog
                watchdog = get_watchdog()
                health_status = watchdog.get_health_status()
                
                # Определяем общий статус
                overall_status = health_status.get('overall', 'unknown')
                components = health_status.get('components', {})
                
                # Формируем ответ
                response = {
                    'status': 'ok' if overall_status == 'healthy' else 'degraded',
                    'overall': overall_status,
                    'components': components,
                    'last_check': health_status.get('last_check'),
                    'crash_count': health_status.get('crash_count', 0),
                    'last_crash': health_status.get('last_crash')
                }
                
                # HTTP статус код зависит от состояния
                http_status = 200 if overall_status == 'healthy' else 503
                
                logger.info(f"[HEALTH] Статус: {overall_status}, компоненты: {list(components.keys())}")
                return jsonify(response), http_status
                
            except ImportError:
                # Watchdog не доступен - возвращаем базовый статус
                logger.warning("[HEALTH] Watchdog не доступен, возвращаем базовый статус")
                return jsonify({'status': 'ok', 'bot': 'running', 'watchdog': 'not_available'}), 200
            except Exception as e:
                logger.error(f"[HEALTH] Ошибка при получении статуса от watchdog: {e}", exc_info=True)
                return jsonify({
                    'status': 'error',
                    'error': str(e),
                    'bot': 'running'
                }), 503
                
        except Exception as e:
            logger.error(f"[HEALTH] Критическая ошибка в health check: {e}", exc_info=True)
            return jsonify({
                'status': 'error',
                'error': str(e)
            }), 503
    
    @app.route('/yookassa/webhook', methods=['POST', 'GET'])
    def yookassa_webhook():
        """Обработчик webhook от ЮKassa (старый путь для совместимости)"""
        return yookassa_webhook_new()
    
    @app.route('/yookassa_webhook', methods=['POST', 'GET'])
    def yookassa_webhook_new():
        """Обработчик webhook от ЮKassa - основной endpoint"""
        if request.method == 'GET':
            # Для проверки доступности endpoint
            logger.info("[YOOKASSA WEBHOOK] GET запрос - проверка доступности endpoint")
            return jsonify({'status': 'ok', 'message': 'YooKassa webhook endpoint is active'}), 200
        
        try:
            logger.info("=" * 80)
            logger.info("[YOOKASSA WEBHOOK] ===== ПОЛУЧЕН ЗАПРОС ОТ ЮKASSA =====")
            logger.info(f"[YOOKASSA WEBHOOK] Headers: {dict(request.headers)}")
            logger.info(f"[YOOKASSA WEBHOOK] Content-Type: {request.content_type}")
            logger.info(f"[YOOKASSA WEBHOOK] Body (первые 1000 символов): {request.get_data(as_text=True)[:1000]}")
            
            event_json = request.get_json(force=True)
            if not event_json:
                logger.warning("[YOOKASSA WEBHOOK] Пустой JSON")
                logger.warning(f"[YOOKASSA WEBHOOK] Raw data: {request.get_data()}")
                return jsonify({'error': 'Empty JSON'}), 400
            
            logger.info(f"[YOOKASSA WEBHOOK] JSON получен: {event_json}")
            logger.info(f"[YOOKASSA WEBHOOK] Событие: {event_json.get('event')}")
            logger.info(f"[YOOKASSA WEBHOOK] Payment ID: {event_json.get('object', {}).get('id')}")
            
            result = process_yookassa_notification(event_json, is_test=False)
            logger.info(f"[YOOKASSA WEBHOOK] Обработка завершена успешно")
            return result
        except Exception as e:
            logger.error(f"[YOOKASSA WEBHOOK] Ошибка: {e}", exc_info=True)
            return jsonify({'error': str(e)}), 500
    
    @app.route('/yookassa/test-webhook', methods=['POST', 'GET'])
    def test_yookassa_webhook():
        """Тестовый endpoint для симуляции уведомлений от ЮKassa"""
        try:
            if request.method == 'GET':
                # Показываем форму для тестирования
                html = """
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Тест webhook ЮKassa</title>
                    <meta charset="UTF-8">
                    <style>
                        body { font-family: Arial, sans-serif; max-width: 800px; margin: 50px auto; padding: 20px; }
                        .form-group { margin: 15px 0; }
                        label { display: block; margin-bottom: 5px; font-weight: bold; }
                        input, select { width: 100%; padding: 8px; box-sizing: border-box; }
                        button { background: #4CAF50; color: white; padding: 10px 20px; border: none; cursor: pointer; }
                        button:hover { background: #45a049; }
                        .result { margin-top: 20px; padding: 15px; background: #f0f0f0; border-radius: 5px; }
                    </style>
                </head>
                <body>
                    <h1>🧪 Тест webhook ЮKassa</h1>
                    <form method="POST" id="testForm">
                        <div class="form-group">
                            <label>YooKassa Payment ID (из БД):</label>
                            <input type="text" name="yookassa_payment_id" placeholder="2c1c5c0a-0001-0000-0000-000000000000" required>
                        </div>
                        <div class="form-group">
                            <label>Событие:</label>
                            <select name="event" required>
                                <option value="payment.succeeded">payment.succeeded</option>
                                <option value="payment.canceled">payment.canceled</option>
                            </select>
                        </div>
                        <button type="submit">Отправить тестовое уведомление</button>
                    </form>
                    <div id="result"></div>
                    <script>
                        document.getElementById('testForm').addEventListener('submit', async function(e) {
                            e.preventDefault();
                            const formData = new FormData(this);
                            const response = await fetch('/yookassa/test-webhook', {
                                method: 'POST',
                                headers: {'Content-Type': 'application/json'},
                                body: JSON.stringify({
                                    yookassa_payment_id: formData.get('yookassa_payment_id'),
                                    event: formData.get('event')
                                })
                            });
                            const result = await response.json();
                            document.getElementById('result').innerHTML = '<div class="result"><pre>' + JSON.stringify(result, null, 2) + '</pre></div>';
                        });
                    </script>
                </body>
                </html>
                """
                return html, 200
            
            # POST запрос - симулируем уведомление
            data = request.json or request.form.to_dict()
            yookassa_payment_id = data.get('yookassa_payment_id')
            event = data.get('event', 'payment.succeeded')
            
            if not yookassa_payment_id:
                return jsonify({'error': 'yookassa_payment_id обязателен'}), 400
            
            logger.info(f"[YOOKASSA TEST] Симуляция события {event} для платежа {yookassa_payment_id}")
            
            # Получаем платеж из БД
            from moviebot.database.db_operations import get_payment_by_yookassa_id
            payment_data = get_payment_by_yookassa_id(yookassa_payment_id)
            
            if not payment_data:
                return jsonify({
                    'error': 'Платеж не найден в БД',
                    'hint': 'Сначала создайте платеж через кнопку "Оплатить" в боте'
                }), 404
            
            # Создаем тестовое уведомление в формате ЮKassa
            test_notification = {
                'type': 'notification',
                'event': event,
                'object': {
                    'id': yookassa_payment_id,
                    'status': 'succeeded' if event == 'payment.succeeded' else 'canceled',
                    'amount': {
                        'value': str(payment_data['amount']),
                        'currency': 'RUB'
                    },
                    'metadata': {
                        'user_id': str(payment_data['user_id']),
                        'chat_id': str(payment_data['chat_id']),
                        'subscription_type': payment_data['subscription_type'],
                        'plan_type': payment_data['plan_type'],
                        'period_type': payment_data['period_type'],
                        'payment_id': payment_data['payment_id']
                    }
                }
            }
            
            # Добавляем group_size в metadata если есть
            if payment_data.get('group_size'):
                test_notification['object']['metadata']['group_size'] = str(payment_data['group_size'])
            
            # Вызываем обработчик уведомления в тестовом режиме
            try:
                result = process_yookassa_notification(test_notification, is_test=True)
                return jsonify({
                    'status': 'success',
                    'message': f'Тестовое уведомление обработано: {event}',
                    'payment_data': {
                        'payment_id': payment_data['payment_id'],
                        'user_id': payment_data['user_id'],
                        'chat_id': payment_data['chat_id'],
                        'amount': float(payment_data['amount']),
                        'status': payment_data['status']
                    },
                    'result': result.get_json() if hasattr(result, 'get_json') else str(result)
                }), 200
            except Exception as e:
                logger.error(f"[YOOKASSA TEST] Ошибка обработки тестового уведомления: {e}", exc_info=True)
                return jsonify({
                    'status': 'error',
                    'error': str(e),
                    'payment_data': payment_data
                }), 500
                
        except Exception as e:
            logger.error(f"[YOOKASSA TEST] Ошибка: {e}", exc_info=True)
            return jsonify({'error': str(e)}), 500
    
    # ========================================================================
    # ⚠️ КРИТИЧНО ДЛЯ RAILWAY: Health check endpoint
    # ========================================================================
    # Railway использует /health для проверки работоспособности контейнера
    # Этот endpoint ОБЯЗАТЕЛЬНО должен возвращать 200 OK
    # НЕ УДАЛЯТЬ и НЕ МЕНЯТЬ логику!
    # ========================================================================
    #@app.route('/health', methods=['GET'])
    #def health():
    #    try:
    #        logger.info("[HEALTH] Healthcheck запрос получен")
    #        return "OK", 200
    ##    except Exception as e:
    #        logger.error(f"[HEALTH] Ошибка в healthcheck: {e}", exc_info=True)
    #        return "ERROR", 500

    logger.info(f"[WEB APP] ===== FLASK ПРИЛОЖЕНИЕ СОЗДАНО =====")
    logger.info(f"[WEB APP] Зарегистрированные роуты: {[str(rule) for rule in app.url_map.iter_rules()]}")
    logger.info(f"[WEB APP] Возвращаем app: {app}")

    # ========================================================================
    # ⚠️ КРИТИЧНО ДЛЯ RAILWAY: Возврат app
    # ========================================================================
    # Функция ОБЯЗАТЕЛЬНО должна возвращать app для запуска на Railway
    # Без этого Railway не сможет запустить веб-сервер
    # ========================================================================
    # Инициализация индекса шазама в фоновом потоке (не блокирует запуск приложения)
    def init_shazam_background():
        try:
            logger.info("[WEB APP] Запуск инициализации индекса шазама в фоновом потоке...")
            init_shazam_index()
            logger.info("[WEB APP] ✅ Инициализация индекса шазама завершена")
        except Exception as e:
            logger.error(f"[WEB APP] ❌ Ошибка при инициализации индекса шазама: {e}", exc_info=True)
    
    thread = threading.Thread(target=init_shazam_background, daemon=True)
    thread.start()
    logger.info("[WEB APP] ✅ Фоновый поток для инициализации индекса шазама запущен")


    # ========================================================================
    # API endpoints для браузерного расширения
    # ========================================================================
    
    # Добавляем after_request hook для автоматического добавления CORS заголовков
    # ВАЖНО: Регистрируем ПЕРЕД определением всех роутов extension API
    # Разрешённые origins для сайта (CORS). Для site API — явно разрешаем production и localhost.
    SITE_ALLOWED_ORIGINS = {
        'https://movie-planner.ru',
        'https://www.movie-planner.ru',
        'http://movie-planner.ru',
        'http://www.movie-planner.ru',
        'http://localhost',
        'http://127.0.0.1',
    }

    @app.after_request
    def after_request(response):
        """Автоматически добавляет CORS заголовки ко всем ответам от extension/site API"""
        if request.path.startswith('/api/extension/') or request.path.startswith('/api/site/'):
            origin = request.headers.get('Origin') or (request.headers.get('Referer') or '')[:80]
            # Для site API разрешаем только известные origins (иначе preflight падает с credentials)
            if request.path.startswith('/api/site/'):
                allow_origin = None
                if origin:
                    origin_clean = origin.split('?')[0].rstrip('/')
                    for allowed in SITE_ALLOWED_ORIGINS:
                        if origin_clean.startswith(allowed.rstrip('/')):
                            allow_origin = origin_clean
                            break
                    if not allow_origin and 'movie-planner' in origin_clean:
                        allow_origin = origin_clean
                if not allow_origin:
                    allow_origin = 'https://movie-planner.ru'
                response.headers['Access-Control-Allow-Origin'] = allow_origin
            else:
                response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization'
            response.headers['Access-Control-Allow-Methods'] = 'GET,POST,OPTIONS'
            response.headers['Access-Control-Allow-Credentials'] = 'true'
            logger.info(f"[CORS] ✅ Добавлены заголовки для пути: {request.path}, метод: {request.method}, статус: {response.status_code}")
        return response
    
    # Функция add_cors_headers больше не используется
    # Все CORS заголовки добавляются автоматически через after_request hook
    # Оставлена для обратной совместимости, но не добавляет заголовки
    def add_cors_headers(response):
        """Заглушка для обратной совместимости - заголовки добавляются через after_request"""
        if response is None:
            response = jsonify({'status': 'ok'})
        # Не добавляем заголовки здесь - after_request hook уже это делает
        return response
    
    @app.route('/api/extension/verify', methods=['GET', 'OPTIONS'])
    def verify_extension_code():
        """Проверка кода расширения и возврат chat_id"""
        # Обработка preflight запроса
        if request.method == 'OPTIONS':
            logger.info("[EXTENSION API] OPTIONS preflight request for /api/extension/verify")
            response = jsonify({'status': 'ok'})
            # after_request hook автоматически добавит CORS заголовки
            return response
        
        logger.info(f"[EXTENSION API] GET /api/extension/verify - code={request.args.get('code', 'NOT_PROVIDED')[:10]}")
        from moviebot.database.db_connection import get_db_connection, get_db_cursor
        
        code = request.args.get('code')
        if not code:
            resp = jsonify({"success": False, "error": "code required"})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 400

        conn = get_db_connection()
        cursor = get_db_cursor()
        try:
            # Без db_lock как просил пользователь
            # Сначала проверяем, существует ли код
            cursor.execute("""
                SELECT chat_id, user_id, expires_at, used FROM extension_links
                WHERE code = %s
            """, (code,))
            row = cursor.fetchone()
            
            if not row:
                # Код не найден
                resp = jsonify({"success": False, "error": "Неверный код"})
                # after_request hook автоматически добавит CORS заголовки
                return resp, 400
            
            # Проверяем, использован ли код
            used = row.get('used') if isinstance(row, dict) else row[3]
            if used:
                resp = jsonify({"success": False, "error": "Неверный код"})
                # after_request hook автоматически добавит CORS заголовки
                return resp, 400
            
            # Проверяем, не истёк ли код
            expires_at = row.get('expires_at') if isinstance(row, dict) else row[2]
            from datetime import datetime
            import pytz
            now = datetime.now(pytz.UTC)
            # Преобразуем expires_at в datetime если нужно
            if isinstance(expires_at, str):
                try:
                    # Пытаемся распарсить ISO формат
                    if 'T' in expires_at or ' ' in expires_at:
                        from dateutil import parser
                        expires_at = parser.parse(expires_at)
                    else:
                        # Простой формат даты
                        expires_at = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                except Exception:
                    # Если не получается - пропускаем проверку срока
                    pass
            # Если это не datetime объект - пропускаем проверку
            if not isinstance(expires_at, datetime):
                # Если не можем определить тип - считаем валидным
                chat_id = row.get('chat_id') if isinstance(row, dict) else row[0]
                user_id = row.get('user_id') if isinstance(row, dict) else row[1]
                cursor.execute("UPDATE extension_links SET used = TRUE WHERE code = %s", (code,))
                conn.commit()
                resp = jsonify({"success": True, "chat_id": chat_id, "user_id": user_id})
                return resp
            if expires_at.tzinfo is None:
                expires_at = pytz.UTC.localize(expires_at)
            
            if expires_at <= now:
                resp = jsonify({"success": False, "error": "Код истёк"})
                # after_request hook автоматически добавит CORS заголовки
                return resp, 400
            
            # Код валиден - используем его
            chat_id = row.get('chat_id') if isinstance(row, dict) else row[0]
            user_id = row.get('user_id') if isinstance(row, dict) else row[1]
            cursor.execute("UPDATE extension_links SET used = TRUE WHERE code = %s", (code,))
            conn.commit()
            resp = jsonify({"success": True, "chat_id": chat_id, "user_id": user_id})
            # after_request hook автоматически добавит CORS заголовки
            return resp
        except Exception as e:
            logger.error("Ошибка проверки кода расширения", exc_info=True)
            resp = jsonify({"success": False, "error": "server error"})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 500
        finally:
            try:
                cursor.close()
            except:
                pass
            try:
                conn.close()
            except:
                pass
    
    @app.route('/api/extension/film-info', methods=['GET', 'OPTIONS'])
    def get_film_info():
        """Получение информации о фильме по kp_id или imdb_id"""
        # Обработка preflight запроса
        if request.method == 'OPTIONS':
            logger.info("[EXTENSION API] OPTIONS preflight request for /api/extension/film-info")
            response = jsonify({'status': 'ok'})
            # Явно добавляем CORS заголовки для OPTIONS (after_request тоже добавит, но для надежности делаем явно)
            response.headers['Access-Control-Allow-Origin'] = '*'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization'
            response.headers['Access-Control-Allow-Methods'] = 'GET,POST,OPTIONS'
            response.headers['Access-Control-Allow-Credentials'] = 'true'
            logger.info("[CORS] ✅ Явно добавлены заголовки для OPTIONS запроса film-info")
            return response
        
        logger.info(f"[EXTENSION API] GET /api/extension/film-info - kp_id={request.args.get('kp_id')}, imdb_id={request.args.get('imdb_id')}, chat_id={request.args.get('chat_id')}")
        from moviebot.api.kinopoisk_api import extract_movie_info, get_film_by_imdb_id
        from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
        import requests
        from moviebot.config import KP_TOKEN
        
        kp_id = request.args.get('kp_id')
        imdb_id = request.args.get('imdb_id')
        chat_id = request.args.get('chat_id', type=int)
        
        if not kp_id and not imdb_id:
            resp = jsonify({"success": False, "error": "kp_id or imdb_id required"})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 400
        
        if not chat_id:
            resp = jsonify({"success": False, "error": "chat_id required"})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 400
        
        try:
            # Если передан imdb_id, конвертируем в kp_id
            if imdb_id and not kp_id:
                film_info = get_film_by_imdb_id(imdb_id)
                if not film_info or not film_info.get('kp_id'):
                    logger.warning(f"[EXTENSION API] Фильм с IMDB ID {imdb_id} не найден в Kinopoisk")
                    resp = jsonify({"success": False, "error": f"Фильм с IMDB ID {imdb_id} не найден в базе Kinopoisk. Возможно, фильм еще не добавлен на Kinopoisk."})
                    # after_request hook автоматически добавит CORS заголовки
                    return resp, 404
                kp_id = film_info.get('kp_id')
                logger.info(f"[EXTENSION API] Конвертирован imdb_id={imdb_id} в kp_id={kp_id}")
            
            # Получаем информацию о фильме через API для определения типа
            logger.info(f"[EXTENSION API] Запрос к Kinopoisk API для kp_id={kp_id}")
            headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
            url_api = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}"
            
            try:
                response = requests.get(url_api, headers=headers, timeout=15)
                logger.info(f"[EXTENSION API] Kinopoisk API ответ: status={response.status_code}")
            except Exception as api_err:
                logger.error(f"[EXTENSION API] Ошибка запроса к Kinopoisk API: {api_err}", exc_info=True)
                return jsonify({"success": False, "error": f"Kinopoisk API error: {str(api_err)}"}), 500
            
            is_series = False
            if response.status_code == 200:
                try:
                    data = response.json()
                    api_type = data.get('type', '').upper()
                    if api_type == 'TV_SERIES':
                        is_series = True
                    logger.info(f"[EXTENSION API] Тип фильма из API: {api_type}, is_series={is_series}")
                except Exception as json_err:
                    logger.error(f"[EXTENSION API] Ошибка парсинга JSON от Kinopoisk API: {json_err}", exc_info=True)
                    return jsonify({"success": False, "error": "Invalid response from Kinopoisk API"}), 500
            else:
                logger.warning(f"[EXTENSION API] Kinopoisk API вернул статус {response.status_code}")
            
            # Формируем правильную ссылку в зависимости от типа
            if is_series:
                link = f"https://www.kinopoisk.ru/series/{kp_id}/"
            else:
                link = f"https://www.kinopoisk.ru/film/{kp_id}/"
            
            logger.info(f"[EXTENSION API] Вызов extract_movie_info для link={link}")
            try:
                info = extract_movie_info(link)
            except Exception as extract_err:
                logger.error(f"[EXTENSION API] Ошибка extract_movie_info: {extract_err}", exc_info=True)
                return jsonify({"success": False, "error": f"Error extracting movie info: {str(extract_err)}"}), 500
            
            if not info:
                logger.warning(f"[EXTENSION API] extract_movie_info вернул None для link={link}")
                return jsonify({"success": False, "error": "film not found"}), 404
            
            logger.info(f"[EXTENSION API] extract_movie_info успешно: title={info.get('title', 'N/A')}")
            
            # Проверяем наличие в базе (без db_lock)
            conn = get_db_connection()
            cursor = get_db_cursor()
            film_in_db = False
            film_id = None
            watched = False
            has_plan = False
            rated = False
            has_unwatched_before = False
            user_id = request.args.get('user_id', type=int)
            
            logger.info(f"[EXTENSION API] Проверка наличия в БД: chat_id={chat_id}, kp_id={kp_id}")
            
            current_episode_watched = False
            next_unwatched_season = None
            next_unwatched_episode = None
            
            with db_lock:
                cursor.execute("""
                    SELECT id, watched FROM movies 
                    WHERE chat_id = %s AND kp_id = %s
                """, (chat_id, str(kp_id)))
                row = cursor.fetchone()
            logger.info(f"[EXTENSION API] Результат запроса БД: row={row}")
            if row:
                film_in_db = True
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                watched = bool(row.get('watched') if isinstance(row, dict) else row[1])
                logger.info(f"[EXTENSION API] Фильм найден в БД: film_id={film_id}, watched={watched}")
            else:
                logger.info(f"[EXTENSION API] Фильм НЕ найден в БД для kp_id={kp_id}")
            
            plan_type = None
            plan_id = None
            if film_id:
                with db_lock:
                    cursor.execute("""
                        SELECT id, plan_type FROM plans 
                        WHERE chat_id = %s AND film_id = %s
                        LIMIT 1
                    """, (chat_id, film_id))
                    plan_row = cursor.fetchone()
                if plan_row:
                    has_plan = True
                    plan_id = plan_row.get('id') if isinstance(plan_row, dict) else plan_row[0]
                    plan_type = plan_row.get('plan_type') if isinstance(plan_row, dict) else plan_row[1]
                
                if user_id:
                    with db_lock:
                        cursor.execute("""
                            SELECT rating FROM ratings 
                            WHERE chat_id = %s AND film_id = %s AND user_id = %s
                        """, (chat_id, film_id, user_id))
                        rating_row = cursor.fetchone()
                    rated = rating_row is not None
                
                if is_series and user_id:
                    current_season = request.args.get('season', type=int)
                    current_episode = request.args.get('episode', type=int)
                    
                    if current_season and current_episode:
                        try:
                            with db_lock:
                                cursor.execute("""
                                    SELECT 1 FROM series_tracking 
                                    WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                                    AND season_number = %s AND episode_number = %s AND watched = TRUE
                                """, (chat_id, film_id, user_id, current_season, current_episode))
                                cur_ep_row = cursor.fetchone()
                            current_episode_watched = cur_ep_row is not None
                        except Exception as unw_err:
                            logger.warning(f"[EXTENSION API] Ошибка проверки current_episode_watched: {unw_err}")
                        
                        unwatched_count = 0
                        try:
                            if current_episode >= 2:
                                with db_lock:
                                    cursor.execute("""
                                        SELECT COUNT(*) as cnt FROM (
                                            SELECT generate_series(1, %s) as ep_num
                                        ) episodes
                                        WHERE NOT EXISTS (
                                            SELECT 1 FROM series_tracking 
                                            WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                                            AND season_number = %s AND episode_number = episodes.ep_num 
                                            AND watched = TRUE
                                        )
                                    """, (current_episode - 1, chat_id, film_id, user_id, current_season))
                                    unwatched_result = cursor.fetchone()
                                if unwatched_result is None:
                                    unwatched_count = 0
                                elif isinstance(unwatched_result, dict):
                                    unwatched_count = int(unwatched_result.get('cnt', 0) or 0)
                                else:
                                    unwatched_count = int(unwatched_result[0] or 0)
                                has_unwatched_before = unwatched_count > 0
                            # Следующая непросмотренная: если текущая отмечена — следующая по счёту, иначе — текущая
                            next_unwatched_season = current_season
                            next_unwatched_episode = (current_episode + 1) if current_episode_watched else current_episode
                            logger.info(f"[EXTENSION API] Серии: film_id={film_id}, s={current_season}, e={current_episode}, current_watched={current_episode_watched}, has_unwatched_before={has_unwatched_before}, next_unwatched={next_unwatched_season}x{next_unwatched_episode}")
                        except Exception as unw_err:
                            logger.warning(f"[EXTENSION API] Ошибка проверки unwatched_before: {unw_err}")
            
            logger.info(f"[EXTENSION API] Возвращаем film-info: film_id={film_id}, film_in_db={film_in_db}, watched={watched}, kp_id={kp_id}, chat_id={chat_id}")
            resp = jsonify({
                "success": True,
                "film": {
                    "kp_id": info.get('kp_id'),
                    "title": info.get('title'),
                    "year": info.get('year'),
                    "is_series": info.get('is_series', False),
                    "genres": info.get('genres'),
                    "director": info.get('director'),
                    "actors": info.get('actors'),
                    "description": info.get('description')
                },
                "in_database": film_in_db,
                "film_id": film_id,
                "watched": watched,
                "rated": rated,
                "has_unwatched_before": has_unwatched_before,
                "current_episode_watched": current_episode_watched,
                "next_unwatched_season": next_unwatched_season,
                "next_unwatched_episode": next_unwatched_episode,
                "has_plan": has_plan,
                "plan_type": plan_type,
                "plan_id": plan_id
            })
            logger.info(f"[EXTENSION API] JSON ответ сформирован: film_id={film_id} (type: {type(film_id)})")
            # after_request hook автоматически добавит CORS заголовки
            return resp
        except Exception as e:
            logger.error(f"[EXTENSION API] Ошибка получения информации о фильме: {e}", exc_info=True)
            import traceback
            logger.error(f"[EXTENSION API] Traceback: {traceback.format_exc()}")
            error_msg = str(e) if e else "server error"
            # Убеждаемся, что error_msg не пустая строка и не "0"
            if not error_msg or error_msg == "0":
                error_msg = "server error"
            resp = jsonify({"success": False, "error": error_msg})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 500
    
    @app.route('/api/extension/add-film', methods=['POST', 'OPTIONS'])
    def add_film_to_database():
        """Добавление фильма в базу данных"""
        # Импорты в начале функции
        from moviebot.api.kinopoisk_api import extract_movie_info
        from moviebot.database.db_connection import get_db_connection, get_db_cursor
        from moviebot.config import KP_TOKEN
        import requests
        
        # Обработка preflight запроса
        if request.method == 'OPTIONS':
            logger.info("[EXTENSION API] OPTIONS preflight request for /api/extension/add-film")
            response = jsonify({'status': 'ok'})
            # after_request hook автоматически добавит CORS заголовки
            return response
        
        logger.info(f"[EXTENSION API] POST /api/extension/add-film - method={request.method}, is_json={request.is_json}, content_type={request.content_type}")
        data = request.get_json() if request.is_json else {}
        logger.info(f"[EXTENSION API] POST /api/extension/add-film - raw_data={data}, kp_id={data.get('kp_id')}, chat_id={data.get('chat_id')}")
        
        # Проверяем, что KP_TOKEN загружен
        if not KP_TOKEN:
            logger.error("[EXTENSION API] KP_TOKEN не установлен")
            resp = jsonify({"success": False, "error": "server configuration error"})
            return resp, 500
        
        if not request.is_json:
            resp = jsonify({"success": False, "error": "JSON required"})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 400
        
        data = request.get_json()
        kp_id = data.get('kp_id')
        chat_id = data.get('chat_id')
        
        # Конвертируем chat_id в int, если он строка
        if chat_id:
            try:
                chat_id = int(chat_id)
            except (ValueError, TypeError):
                resp = jsonify({"success": False, "error": "chat_id must be a number"})
                return resp, 400
        
        if not kp_id or not chat_id:
            resp = jsonify({"success": False, "error": "kp_id and chat_id required"})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 400
        
        try:
            # Сначала определяем тип через API, чтобы правильно сформировать ссылку
            headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
            url_api = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}"
            response = requests.get(url_api, headers=headers, timeout=15)
            
            is_series = False
            if response.status_code == 200:
                api_data = response.json()
                api_type = api_data.get('type', '').upper()
                if api_type in ['TV_SERIES', 'MINI_SERIES']:
                    is_series = True
            
            # Формируем правильную ссылку на основе типа
            link_type = 'series' if is_series else 'film'
            link = f"https://www.kinopoisk.ru/{link_type}/{kp_id}/"
            
            # Используем extract_movie_info для получения информации, включая is_series
            info = extract_movie_info(link)
            
            if not info:
                resp = jsonify({"success": False, "error": "film not found"})
                # after_request hook автоматически добавит CORS заголовки
                return resp, 404
            
            # Используем is_series из API, а не из extract_movie_info (более надежно)
            # Но если extract_movie_info тоже определил, используем его значение
            if info.get('is_series') is not None:
                is_series = info.get('is_series', False)
            
            conn = get_db_connection()
            cursor = get_db_cursor()
            # Без db_lock как просил пользователь
            online_link = data.get('online_link')
            cursor.execute("""
                INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series, online_link)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chat_id, kp_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    year = EXCLUDED.year,
                    genres = EXCLUDED.genres,
                    description = EXCLUDED.description,
                    director = EXCLUDED.director,
                    actors = EXCLUDED.actors,
                    is_series = EXCLUDED.is_series,
                    link = EXCLUDED.link,
                    online_link = COALESCE(EXCLUDED.online_link, movies.online_link)
                RETURNING id
            """, (
                chat_id, link, str(kp_id), info.get('title'), info.get('year'),
                info.get('genres', '—'), info.get('description', 'Нет описания'),
                info.get('director', 'Не указан'), info.get('actors', '—'),
                1 if is_series else 0, online_link
            ))
            result = cursor.fetchone()
            film_id = result.get('id') if isinstance(result, dict) else result[0]
            conn.commit()

            # Отправляем сообщение в Telegram о добавлении фильма
            try:
                from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                title = info.get('title', 'Неизвестный фильм')
                year = info.get('year', '')
                type_emoji = "📺" if is_series else "🎬"
                type_text = "Сериал" if is_series else "Фильм"
                text = f"{type_emoji} <b>{title}</b> ({year})\n\n✅ {type_text} добавлен в базу через расширение браузера"
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("📖 К описанию", callback_data=f"show_film:{kp_id}"))
                bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup)
                logger.info(f"[EXTENSION API] Сообщение о добавлении фильма отправлено в chat_id={chat_id}")
            except Exception as e:
                logger.error(f"[EXTENSION API] Ошибка отправки сообщения о добавлении фильма: {e}", exc_info=True)

            resp = jsonify({"success": True, "film_id": film_id})
            return resp
        except Exception as e:
            logger.error(f"Ошибка добавления фильма в базу: {str(e)}", exc_info=True)
            resp = jsonify({"success": False, "error": f"server error: {str(e)}"})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 500
    
    @app.route('/api/extension/delete-film', methods=['POST', 'OPTIONS'])
    def delete_film():
        """Удаление фильма из базы данных"""
        # Обработка preflight запроса
        if request.method == 'OPTIONS':
            logger.info("[EXTENSION API] OPTIONS preflight request for /api/extension/delete-film")
            response = jsonify({'status': 'ok'})
            # after_request hook автоматически добавит CORS заголовки
            return response
        
        if not request.is_json:
            resp = jsonify({"success": False, "error": "JSON required"})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 400
        
        data = request.get_json()
        kp_id = data.get('kp_id')
        chat_id = data.get('chat_id')
        
        # Конвертируем chat_id в int, если он строка
        if chat_id:
            try:
                chat_id = int(chat_id)
            except (ValueError, TypeError):
                resp = jsonify({"success": False, "error": "chat_id must be a number"})
                return resp, 400
        
        if not kp_id or not chat_id:
            resp = jsonify({"success": False, "error": "kp_id and chat_id required"})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 400
        
        try:
            from moviebot.database.db_connection import get_db_connection, get_db_cursor
            conn = get_db_connection()
            cursor = get_db_cursor()
            cursor.execute("SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, str(kp_id)))
            film_row = cursor.fetchone()
            if not film_row:
                resp = jsonify({"success": False, "error": "film not found"})
                return resp, 404

            film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
            cursor.execute("SELECT title, kp_id, link FROM movies WHERE id = %s AND chat_id = %s", (film_id, chat_id))
            film_info_row = cursor.fetchone()
            title_before_delete = None
            kp_id_for_notification = None
            link_before_delete = None
            if film_info_row:
                title_before_delete = film_info_row.get('title') if isinstance(film_info_row, dict) else film_info_row[0]
                kp_id_for_notification = film_info_row.get('kp_id') if isinstance(film_info_row, dict) else film_info_row[1]
                link_before_delete = film_info_row.get('link') if isinstance(film_info_row, dict) else film_info_row[2]

            cursor.execute('DELETE FROM ratings WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
            cursor.execute('DELETE FROM plans WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
            cursor.execute('DELETE FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
            conn.commit()

            if title_before_delete and kp_id_for_notification:
                try:
                    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                    is_series_del = '/series/' in link_before_delete if link_before_delete else False
                    type_emoji = "📺" if is_series_del else "🎬"
                    text = f"{type_emoji} <b>{title_before_delete}</b>\n\n❌ Фильм удален из базы через расширение браузера"
                    markup = InlineKeyboardMarkup()
                    markup.add(InlineKeyboardButton("📖 К описанию", callback_data=f"show_film:{kp_id_for_notification}"))
                    bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup)
                    logger.info(f"[EXTENSION API] Сообщение об удалении фильма отправлено в chat_id={chat_id}")
                except Exception as e:
                    logger.error(f"[EXTENSION API] Ошибка отправки сообщения об удалении фильма: {e}", exc_info=True)

            resp = jsonify({"success": True})
            return resp
        except Exception as e:
            logger.error("Ошибка удаления фильма из базы", exc_info=True)
            resp = jsonify({"success": False, "error": "server error"})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 500
    
    @app.route('/api/extension/parse-time', methods=['POST', 'OPTIONS'])
    def parse_time():
        """Парсинг 'человеческого' времени (завтра вечером, пятница и т.д.)"""
        # Обработка preflight запроса
        if request.method == 'OPTIONS':
            logger.info("[EXTENSION API] OPTIONS preflight request for /api/extension/parse-time")
            response = jsonify({'status': 'ok'})
            # after_request hook автоматически добавит CORS заголовки
            return response
        
        if not request.is_json:
            resp = jsonify({"success": False, "error": "JSON required"})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 400
        
        data = request.get_json()
        time_text = data.get('time_text')
        user_id = data.get('user_id')
        
        # Конвертируем user_id в int, если он строка
        if user_id:
            try:
                user_id = int(user_id)
            except (ValueError, TypeError):
                resp = jsonify({"success": False, "error": "user_id must be a number"})
                return resp, 400
        
        if not time_text:
            resp = jsonify({"success": False, "error": "time_text required"})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 400
        
        if not user_id:
            resp = jsonify({"success": False, "error": "user_id required"})
            return resp, 400
        
        try:
            from moviebot.utils.parsing import parse_plan_date_text
            parsed_dt = parse_plan_date_text(time_text, user_id)
            
            if parsed_dt:
                # Конвертируем в ISO формат
                resp = jsonify({"success": True, "datetime": parsed_dt.isoformat()})
                # after_request hook автоматически добавит CORS заголовки
                return resp
            else:
                resp = jsonify({"success": False, "error": "Could not parse time"})
                # after_request hook автоматически добавит CORS заголовки
                return resp, 400
        except Exception as e:
            logger.error("Ошибка парсинга времени", exc_info=True)
            resp = jsonify({"success": False, "error": "server error"})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 500
    
    @app.route('/api/extension/create-plan', methods=['POST', 'OPTIONS'])
    def create_plan():
        """Создание или обновление плана просмотра"""
        # Обработка preflight запроса
        if request.method == 'OPTIONS':
            logger.info("[EXTENSION API] OPTIONS preflight request for /api/extension/create-plan")
            response = jsonify({'status': 'ok'})
            # after_request hook автоматически добавит CORS заголовки
            return response
        
        data = request.get_json() if request.is_json else {}
        logger.info(f"[EXTENSION API] POST /api/extension/create-plan - chat_id={data.get('chat_id')}, film_id={data.get('film_id')}")
        from moviebot.database.db_connection import get_db_connection, get_db_cursor
        from datetime import datetime
        import pytz
        
        if not request.is_json:
            resp = jsonify({"success": False, "error": "JSON required"})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 400
        
        data = request.get_json()
        chat_id = data.get('chat_id')
        film_id = data.get('film_id')
        plan_type = data.get('plan_type')  # 'home' or 'cinema'
        plan_datetime = data.get('plan_datetime')  # ISO format
        
        # Конвертируем в int, если они строки
        if chat_id:
            try:
                chat_id = int(chat_id)
            except (ValueError, TypeError):
                resp = jsonify({"success": False, "error": "chat_id must be a number"})
                return resp, 400
        if film_id:
            try:
                film_id = int(film_id)
            except (ValueError, TypeError):
                resp = jsonify({"success": False, "error": "film_id must be a number"})
                return resp, 400
        
        user_id = data.get('user_id')
        if user_id:
            try:
                user_id = int(user_id)
            except (ValueError, TypeError):
                resp = jsonify({"success": False, "error": "user_id must be a number"})
                return resp, 400
        
        streaming_service = data.get('streaming_service')
        streaming_url = data.get('streaming_url')
        
        if not all([chat_id, film_id, plan_type, plan_datetime, user_id]):
            resp = jsonify({"success": False, "error": "missing required fields"})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 400
        
        try:
            # Парсим datetime
            dt = datetime.fromisoformat(plan_datetime.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = pytz.UTC.localize(dt)
            
            conn = get_db_connection()
            cursor = get_db_cursor()
            try:
                # Без db_lock как просил пользователь
                # Проверяем, есть ли уже план для этого фильма
                cursor.execute("""
                    SELECT id FROM plans 
                    WHERE chat_id = %s AND film_id = %s
                """, (chat_id, film_id))
                existing_plan = cursor.fetchone()
                
                if existing_plan:
                    # Обновляем существующий план
                    plan_id = existing_plan.get('id') if isinstance(existing_plan, dict) else existing_plan[0]
                    cursor.execute("""
                        UPDATE plans 
                        SET plan_type = %s, plan_datetime = %s, streaming_service = %s, streaming_url = %s
                        WHERE id = %s
                    """, (plan_type, dt, streaming_service, streaming_url, plan_id))
                    logger.info(f"[EXTENSION API] План обновлен: plan_id={plan_id}")
                else:
                    # Создаем новый план
                    cursor.execute("""
                        INSERT INTO plans (chat_id, film_id, plan_type, plan_datetime, user_id, streaming_service, streaming_url)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (chat_id, film_id, plan_type, dt, user_id, streaming_service, streaming_url))
                    result = cursor.fetchone()
                    plan_id = result.get('id') if isinstance(result, dict) else result[0]
                    logger.info(f"[EXTENSION API] План создан: plan_id={plan_id}")
                
                conn.commit()
                
                # Отправляем сообщение в Telegram о создании/обновлении плана
                try:
                    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                    from datetime import datetime
                    import pytz
                    
                    # Получаем информацию о фильме
                    cursor.execute("SELECT title, kp_id, link, is_series FROM movies WHERE id = %s AND chat_id = %s", (film_id, chat_id))
                    film_row = cursor.fetchone()
                    if film_row:
                        title = film_row.get('title') if isinstance(film_row, dict) else film_row[0]
                        kp_id_plan = film_row.get('kp_id') if isinstance(film_row, dict) else film_row[1]
                        link = film_row.get('link') if isinstance(film_row, dict) else film_row[2]
                        is_series_db = film_row.get('is_series') if isinstance(film_row, dict) else film_row[3]
                        
                        # Определяем тип (проверяем и поле is_series, и ссылку для надежности)
                        is_series_plan = bool(is_series_db) or ('/series/' in link if link else False)
                        type_emoji = "📺" if is_series_plan else "🎬"
                        type_text = "Сериал" if is_series_plan else "Фильм"
                        
                        # Форматируем дату и время
                        moscow_tz = pytz.timezone('Europe/Moscow')
                        dt_moscow = dt.astimezone(moscow_tz)
                        date_str = dt_moscow.strftime('%d.%m.%Y')
                        time_str = dt_moscow.strftime('%H:%M')
                        
                        plan_type_text = "дома" if plan_type == 'home' else "в кино"
                        action_text = "обновлен" if existing_plan else "создан"
                        
                        text = f"{type_emoji} <b>{title}</b>\n\n📅 План просмотра {type_text.lower()}а {action_text}:\n• {plan_type_text}\n• {date_str} в {time_str}"
                        
                        markup = InlineKeyboardMarkup()
                        markup.add(InlineKeyboardButton("📖 К описанию", callback_data=f"show_film:{kp_id_plan}"))
                        
                        # Если план "в кино", добавляем кнопку "Добавить билеты"
                        if plan_type == 'cinema':
                            from moviebot.utils.helpers import has_tickets_access
                            if has_tickets_access(chat_id, user_id):
                                markup.add(InlineKeyboardButton("🎟️ Добавить билеты", callback_data=f"add_ticket:{plan_id}"))
                        
                        bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup)
                        logger.info(f"[EXTENSION API] Сообщение о создании/обновлении плана отправлено в chat_id={chat_id}")
                except Exception as e:
                    logger.error(f"[EXTENSION API] Ошибка отправки сообщения о плане: {e}", exc_info=True)
                    # Не прерываем выполнение, если не удалось отправить сообщение
                
                resp = jsonify({"success": True, "plan_id": plan_id})
                # after_request hook автоматически добавит CORS заголовки
                return resp
            finally:
                try:
                    cursor.close()
                except:
                    pass
                try:
                    conn.close()
                except:
                    pass
        except Exception as e:
            logger.error("Ошибка создания плана", exc_info=True)
            resp = jsonify({"success": False, "error": "server error"})
            # after_request hook автоматически добавит CORS заголовки
            return resp, 500
    
    @app.route('/api/extension/search', methods=['GET', 'OPTIONS'])
    def search_films_endpoint():
        """Поиск фильмов для расширения"""
        # Обработка preflight запроса
        if request.method == 'OPTIONS':
            logger.info("[EXTENSION API] OPTIONS preflight request for /api/extension/search")
            response = jsonify({'status': 'ok'})
            return response
        
        query = request.args.get('query')
        page = request.args.get('page', 1, type=int)
        
        if not query:
            resp = jsonify({"success": False, "error": "query required"})
            return resp, 400
        
        try:
            from moviebot.api.kinopoisk_api import search_films
            items, total_pages = search_films(query, page)
            
            # Форматируем результаты
            results = []
            for item in items:
                kp_id = item.get('kinopoiskId') or item.get('filmId')
                name_ru = item.get('nameRu') or ''
                name_en = item.get('nameEn') or ''
                year = item.get('year')
                type_film = item.get('type', 'FILM').upper()
                is_series = type_film == 'TV_SERIES'
                
                results.append({
                    'kp_id': str(kp_id) if kp_id else None,
                    'title': name_ru or name_en,
                    'title_en': name_en if name_ru else None,
                    'year': year,
                    'is_series': is_series
                })
            
            resp = jsonify({
                "success": True,
                "results": results,
                "page": page,
                "total_pages": total_pages
            })
            return resp
        except Exception as e:
            logger.error(f"Ошибка поиска фильмов: {e}", exc_info=True)
            resp = jsonify({"success": False, "error": "server error"})
            return resp, 500
    
    @app.route('/api/extension/init-ticket-upload', methods=['POST', 'OPTIONS'])
    def init_ticket_upload():
        """Инициализация загрузки билетов для плана"""
        # Обработка preflight запроса
        if request.method == 'OPTIONS':
            logger.info("[EXTENSION API] OPTIONS preflight request for /api/extension/init-ticket-upload")
            response = jsonify({'status': 'ok'})
            return response
        
        data = request.get_json() if request.is_json else {}
        chat_id = data.get('chat_id')
        user_id = data.get('user_id')
        plan_id = data.get('plan_id')
        
        if not chat_id or not user_id or not plan_id:
            resp = jsonify({"success": False, "error": "chat_id, user_id and plan_id required"})
            return resp, 400
        
        try:
            from moviebot.utils.helpers import has_tickets_access
            from moviebot.states import user_ticket_state
            from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
            from moviebot.bot.bot_init import bot
            
            # Проверяем подписку
            if not has_tickets_access(chat_id, user_id):
                resp = jsonify({"success": False, "error": "Нет доступа к билетам. Нужна подписка 'Билеты'."})
                return resp, 403
            
            # Устанавливаем состояние для загрузки билетов
            user_ticket_state[user_id] = {
                'step': 'upload_ticket',
                'plan_id': plan_id,
                'chat_id': chat_id,
                'created_at': time.time()
            }
            
            # Создаем клавиатуру с отменой
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data=f"cancel_ticket_upload:{plan_id}"))
            
            # Отправляем сообщение в бот
            text = "🎟️ <b>Загрузка билетов</b>\n\nОтправьте фото или файл с билетом(ами).\n\n💡 В группе отправьте в ответ на это сообщение, в личке можно отправить следующим сообщением."
            
            sent_msg = bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode='HTML',
                reply_markup=markup
            )
            
            # Сохраняем message_id для проверки реплаев в групповых чатах
            if sent_msg:
                user_ticket_state[user_id]['prompt_message_id'] = sent_msg.message_id
            
            logger.info(f"[EXTENSION API] Инициирована загрузка билетов для plan_id={plan_id}, chat_id={chat_id}, user_id={user_id}")
            
            resp = jsonify({"success": True, "message_id": sent_msg.message_id if sent_msg else None})
            return resp
        except Exception as e:
            logger.error(f"[EXTENSION API] Ошибка инициализации загрузки билетов: {e}", exc_info=True)
            resp = jsonify({"success": False, "error": "server error"})
            return resp, 500
    
    @app.route('/api/extension/check-subscription', methods=['GET', 'OPTIONS'])
    def check_subscription():
        """Проверка подписки пользователя/группы"""
        # Обработка preflight запроса
        if request.method == 'OPTIONS':
            logger.info("[EXTENSION API] OPTIONS preflight request for /api/extension/check-subscription")
            response = jsonify({'status': 'ok'})
            return response
        
        chat_id = request.args.get('chat_id', type=int)
        user_id = request.args.get('user_id', type=int)
        
        if not chat_id or not user_id:
            resp = jsonify({"success": False, "error": "chat_id and user_id required"})
            return resp, 400
        
        try:
            from moviebot.utils.helpers import has_tickets_access, has_notifications_access
            has_tickets = has_tickets_access(chat_id, user_id)
            has_notifications = has_notifications_access(chat_id, user_id)
            
            resp = jsonify({
                "success": True,
                "has_tickets_access": has_tickets,
                "has_notifications_access": has_notifications
            })
            return resp
        except Exception as e:
            logger.error(f"Ошибка проверки подписки: {e}", exc_info=True)
            resp = jsonify({"success": False, "error": "server error"})
            return resp, 500
    
    @app.route('/api/extension/streaming-services', methods=['GET', 'OPTIONS'])
    def get_streaming_services():
        """Получение списка стриминговых сервисов для фильма"""
        # Обработка preflight запроса
        if request.method == 'OPTIONS':
            logger.info("[EXTENSION API] OPTIONS preflight request for /api/extension/streaming-services")
            response = jsonify({'status': 'ok'})
            # after_request hook автоматически добавит CORS заголовки
            return response
        
        kp_id = request.args.get('kp_id')
        if not kp_id:
            resp = jsonify({"success": False, "error": "kp_id required"})
            return resp, 400
        
        try:
            from moviebot.api.kinopoisk_api import get_external_sources
            sources = get_external_sources(int(kp_id))
            
            # Форматируем список сервисов
            services = []
            if sources:
                for platform, url in sources:
                    services.append({
                        'name': platform,
                        'url': url
                    })
            
            resp = jsonify({"success": True, "services": services})
            # after_request hook автоматически добавит CORS заголовки
            return resp
        except Exception as e:
            logger.error(f"Ошибка получения стриминговых сервисов: {e}", exc_info=True)
            resp = jsonify({"success": False, "error": "server error"})
            return resp, 500
    
    @app.route('/api/extension/search-film-by-keyword', methods=['GET', 'OPTIONS'])
    def search_film_by_keyword():
        """Поиск фильма по keyword и году (использует API v2.1 с форматом 'название год')"""
        # Обработка preflight запроса
        if request.method == 'OPTIONS':
            logger.info("[EXTENSION API] OPTIONS preflight request for /api/extension/search-film-by-keyword")
            response = jsonify({'status': 'ok'})
            return response
        
        keyword = request.args.get('keyword')
        year = request.args.get('year', type=int)
        search_type = request.args.get('type', '').upper()  # FILM или TV_SERIES
        
        if not keyword:
            resp = jsonify({"success": False, "error": "keyword required"})
            return resp, 400
        
        try:
            from moviebot.config import KP_TOKEN
            import requests
            
            headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
            
            # Как в боте /search и Letterboxd fallback: "название год" даёт лучшие результаты
            base = keyword.strip()
            if year is not None:
                search_query = f"{base} {year}".strip()
            else:
                search_query = base
            
            url = "https://kinopoiskapiunofficial.tech/api/v2.1/films/search-by-keyword"
            params = {'keyword': search_query, 'page': 1}
            
            logger.info(f"[EXTENSION API] Поиск фильма: keyword={search_query} (base={base}, year={year}), type={search_type}")
            
            response = requests.get(url, headers=headers, params=params, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                films = data.get('films', [])
                logger.info(f"[EXTENSION API] Найдено результатов: {len(films)}")
                
                # Фильтруем по типу, если указан
                if search_type:
                    if search_type == 'TV_SERIES':
                        films = [f for f in films if f.get('type', '').upper() in ['TV_SERIES', 'MINI_SERIES']]
                        logger.info(f"[EXTENSION API] После фильтрации по типу TV_SERIES: {len(films)}")
                    elif search_type == 'FILM':
                        films = [f for f in films if f.get('type', '').upper() == 'FILM']
                        logger.info(f"[EXTENSION API] После фильтрации по типу FILM: {len(films)}")
                
                # Фильтруем по году, если указан (API возвращает year как строка "2026")
                if year is not None and films:
                    year_str = str(year)
                    year_matched = [f for f in films if str(f.get('year') or '').strip() == year_str]
                    if year_matched:
                        films = year_matched
                        logger.info(f"[EXTENSION API] После фильтрации по году {year}: {len(films)}")
                    else:
                        logger.warning(f"[EXTENSION API] Нет фильмов с годом {year}, используем все результаты без фильтра по году")
                
                if films and len(films) > 0:
                    # Берем первый результат
                    film = films[0]
                    kp_id = film.get('filmId')
                    type_film = film.get('type', 'FILM').upper()
                    is_series = type_film in ['TV_SERIES', 'MINI_SERIES']
                    
                    logger.info(f"[EXTENSION API] Найден фильм: kp_id={kp_id}, nameRu={film.get('nameRu')}, year={film.get('year')}, type={type_film}")
                    
                    # Формируем правильную ссылку
                    link_type = 'series' if is_series else 'film'
                    link = f"https://www.kinopoisk.ru/{link_type}/{kp_id}/"
                    
                    resp = jsonify({
                        "success": True,
                        "kp_id": str(kp_id) if kp_id else None,
                        "film": {
                            "kinopoiskId": kp_id,
                            "nameRu": film.get('nameRu'),
                            "nameEn": film.get('nameEn'),
                            "nameOriginal": film.get('nameEn') or film.get('nameRu'),
                            "year": film.get('year'),
                            "type": type_film,
                            "is_series": is_series,
                            "link": link
                        }
                    })
                    return resp
                else:
                    logger.warning(f"[EXTENSION API] Фильм не найден после фильтрации: keyword={search_query}, year={year}, type={search_type}, найдено результатов={len(data.get('films', []))}")
                    resp = jsonify({"success": False, "error": "film not found"})
                    return resp, 404
            else:
                resp = jsonify({"success": False, "error": "api error"})
                return resp, 500
        except Exception as e:
            logger.error(f"Ошибка поиска фильма по keyword: {e}", exc_info=True)
            resp = jsonify({"success": False, "error": "server error"})
            return resp, 500
    
    @app.route('/api/extension/mark-episode', methods=['POST', 'OPTIONS'])
    def mark_episode():
        """Отметка серии как просмотренной"""
        if request.method == 'OPTIONS':
            logger.info("[EXTENSION API] OPTIONS preflight request for /api/extension/mark-episode")
            response = jsonify({'status': 'ok'})
            return response
        
        try:
            data = request.get_json()
            chat_id = data.get('chat_id')
            user_id = data.get('user_id')
            kp_id = data.get('kp_id')
            film_id = data.get('film_id')
            season = data.get('season')
            if season is not None:
                try:
                    season = int(season)
                except (ValueError, TypeError):
                    season = None
            episode = data.get('episode')
            if episode is not None:
                try:
                    episode = int(episode)
                except (ValueError, TypeError):
                    episode = None
            mark_all_previous = data.get('mark_all_previous', False)
            online_link = data.get('online_link')
            
            if not all([chat_id, user_id, kp_id, season, episode]):
                return jsonify({"success": False, "error": "missing required fields"}), 400
            
            from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
            
            conn = get_db_connection()
            cursor = get_db_cursor()

            if not film_id:
                with db_lock:
                    cursor.execute("SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, str(kp_id)))
                    row = cursor.fetchone()
                    if row:
                        film_id = row.get('id') if isinstance(row, dict) else row[0]

                if not film_id:
                    from moviebot.api.kinopoisk_api import extract_movie_info
                    from moviebot.config import KP_TOKEN
                    import requests
                    headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
                    url_api = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}"
                    response = requests.get(url_api, headers=headers, timeout=15)
                    if response.status_code == 200:
                        api_data = response.json()
                        api_type = api_data.get('type', '').upper()
                        is_series = api_type in ['TV_SERIES', 'MINI_SERIES']
                        link = f"https://www.kinopoisk.ru/series/{kp_id}/" if is_series else f"https://www.kinopoisk.ru/film/{kp_id}/"
                        info = extract_movie_info(link)
                        if info:
                            with db_lock:
                                cursor.execute("""
                                    INSERT INTO movies (chat_id, kp_id, title, year, link, is_series, online_link)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    RETURNING id
                                """, (chat_id, str(kp_id), info.get('title'), info.get('year'), link, 1 if is_series else 0, online_link))
                                row = cursor.fetchone()
                                film_id = row.get('id') if isinstance(row, dict) else (row[0] if row else None)
                                conn.commit()

            if not film_id:
                return jsonify({"success": False, "error": "film not found"}), 404

            if online_link:
                with db_lock:
                    cursor.execute("UPDATE movies SET online_link = %s WHERE id = %s AND chat_id = %s", (online_link, film_id, chat_id))
                    conn.commit()

            with db_lock:
                if mark_all_previous:
                    for ep_num in range(1, episode + 1):
                        cursor.execute("""
                            INSERT INTO series_tracking (chat_id, film_id, user_id, season_number, episode_number, watched, watched_date)
                            VALUES (%s, %s, %s, %s, %s, TRUE, CURRENT_TIMESTAMP)
                            ON CONFLICT (chat_id, film_id, user_id, season_number, episode_number)
                            DO UPDATE SET watched = TRUE, watched_date = CURRENT_TIMESTAMP
                        """, (chat_id, film_id, user_id, season, ep_num))
                else:
                    cursor.execute("""
                        INSERT INTO series_tracking (chat_id, film_id, user_id, season_number, episode_number, watched, watched_date)
                        VALUES (%s, %s, %s, %s, %s, TRUE, CURRENT_TIMESTAMP)
                        ON CONFLICT (chat_id, film_id, user_id, season_number, episode_number)
                        DO UPDATE SET watched = TRUE, watched_date = CURRENT_TIMESTAMP
                    """, (chat_id, film_id, user_id, season, episode))
                conn.commit()

            try:
                from moviebot.bot.handlers.series import send_episode_marked_message
                send_episode_marked_message(bot, chat_id, user_id, kp_id, film_id, season, episode, mark_all_previous)
            except Exception as e:
                logger.error(f"[EXTENSION API] Ошибка отправки сообщения в бота: {e}", exc_info=True)

            return jsonify({"success": True})
        except Exception as e:
            logger.error(f"[EXTENSION API] Ошибка отметки серии: {e}", exc_info=True)
            error_msg = str(e) if e else "server error"
            return jsonify({"success": False, "error": error_msg}), 500
    
    @app.route('/api/extension/mark-film-watched', methods=['POST', 'OPTIONS'])
    def mark_film_watched():
        """Отметка фильма как просмотренного"""
        if request.method == 'OPTIONS':
            logger.info("[EXTENSION API] OPTIONS preflight request for /api/extension/mark-film-watched")
            response = jsonify({'status': 'ok'})
            return response
        
        try:
            data = request.get_json()
            chat_id = data.get('chat_id')
            user_id = data.get('user_id')
            kp_id = data.get('kp_id')
            film_id = data.get('film_id')
            online_link = data.get('online_link')
            
            if not all([chat_id, user_id, kp_id]):
                return jsonify({"success": False, "error": "missing required fields"}), 400
            
            from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
            
            conn = get_db_connection()
            cursor = get_db_cursor()

            if not film_id:
                with db_lock:
                    cursor.execute("SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, str(kp_id)))
                    row = cursor.fetchone()
                if row:
                    film_id = row.get('id') if isinstance(row, dict) else row[0]
                else:
                    return jsonify({"success": False, "error": "film not found"}), 404

            with db_lock:
                if online_link:
                    cursor.execute("""
                        UPDATE movies SET watched = 1, online_link = %s WHERE id = %s AND chat_id = %s
                    """, (online_link, film_id, chat_id))
                else:
                    cursor.execute("""
                        UPDATE movies SET watched = 1 WHERE id = %s AND chat_id = %s
                    """, (film_id, chat_id))
                conn.commit()

            try:
                from moviebot.bot.handlers.text_messages import send_film_watched_message
                send_film_watched_message(bot, chat_id, user_id, kp_id, film_id)
            except Exception as e:
                logger.error(f"[EXTENSION API] Ошибка отправки сообщения в бота: {e}", exc_info=True)

            return jsonify({"success": True})
        except Exception as e:
            logger.error(f"[EXTENSION API] Ошибка отметки фильма: {e}", exc_info=True)
            return jsonify({"success": False, "error": "server error"}), 500
    
    @app.route('/api/extension/rate-film', methods=['POST', 'OPTIONS'])
    def rate_film():
        """Оценка фильма"""
        if request.method == 'OPTIONS':
            logger.info("[EXTENSION API] OPTIONS preflight request for /api/extension/rate-film")
            response = jsonify({'status': 'ok'})
            return response
        
        try:
            data = request.get_json()
            chat_id = data.get('chat_id')
            user_id = data.get('user_id')
            kp_id = data.get('kp_id')
            film_id = data.get('film_id')
            rating = data.get('rating')
            if rating is not None:
                try:
                    rating = int(rating)
                except (ValueError, TypeError):
                    rating = None
            
            if not all([chat_id, user_id, kp_id, rating]) or not (1 <= rating <= 10):
                return jsonify({"success": False, "error": "invalid rating"}), 400
            
            from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
            
            conn = get_db_connection()
            cursor = get_db_cursor()

            if not film_id:
                with db_lock:
                    cursor.execute("SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, str(kp_id)))
                    row = cursor.fetchone()
                    if row:
                        film_id = row.get('id') if isinstance(row, dict) else row[0]
                if not film_id:
                    return jsonify({"success": False, "error": "film not found"}), 404

            with db_lock:
                cursor.execute("""
                    INSERT INTO ratings (chat_id, film_id, user_id, rating, is_imported)
                    VALUES (%s, %s, %s, %s, FALSE)
                    ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET rating = %s, is_imported = FALSE
                """, (chat_id, film_id, user_id, rating, rating))
                conn.commit()

            cursor.execute("SELECT title FROM movies WHERE id = %s AND chat_id = %s", (film_id, chat_id))
            film_row = cursor.fetchone()
            film_title = film_row.get('title') if isinstance(film_row, dict) else (film_row[0] if film_row else None)

            try:
                from moviebot.bot.callbacks.film_callbacks import send_rating_message
                send_rating_message(bot, chat_id, user_id, kp_id, film_id, rating, film_title)
            except Exception as e:
                logger.error(f"[EXTENSION API] Ошибка отправки сообщения в бота: {e}", exc_info=True)

            recommendations_sent = False
            if rating >= 7:
                try:
                    from moviebot.api.kinopoisk_api import get_similars
                    similar = get_similars(kp_id)
                    if similar:
                        from moviebot.bot.callbacks.film_callbacks import send_recommendations_message
                        send_recommendations_message(bot, chat_id, user_id, kp_id, similar, source_film_title=film_title)
                        recommendations_sent = True
                except Exception as e:
                    logger.error(f"[EXTENSION API] Ошибка получения рекомендаций: {e}", exc_info=True)

            return jsonify({"success": True, "recommendations": recommendations_sent})
        except Exception as e:
            logger.error(f"[EXTENSION API] Ошибка оценки фильма: {e}", exc_info=True)
            return jsonify({"success": False, "error": "server error"}), 500

    # ========== API для сайта (личный кабинет) ==========
    import secrets
    from datetime import datetime, timedelta
    import pytz

    def _site_token_to_chat_id():
        """Из заголовка Authorization: Bearer <token> возвращает chat_id или None."""
        auth = request.headers.get('Authorization')
        if not auth or not auth.startswith('Bearer '):
            return None
        token = auth[7:].strip()
        if not token:
            return None
        from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
        conn = get_db_connection()
        cur = get_db_cursor()
        try:
            with db_lock:
                cur.execute(
                    "SELECT chat_id FROM site_sessions WHERE token = %s AND expires_at > %s",
                    (token, datetime.now(pytz.UTC))
                )
                row = cur.fetchone()
            if row:
                return row.get('chat_id') if isinstance(row, dict) else row[0]
        except Exception as e:
            logger.error(f"[SITE API] Ошибка проверки токена: {e}", exc_info=True)
        return None

    @app.route('/api/site/config', methods=['GET', 'OPTIONS'])
    def site_config():
        """Публичный конфиг: ссылки на расширения Chrome/Opera из env (для кнопки «Установить расширение»)."""
        if request.method == 'OPTIONS':
            return jsonify({'status': 'ok'})
        default_ext = 'https://chromewebstore.google.com/detail/movie-planner-bot/fldeclcfcngcjphhklommcebkpfipdol'
        chrome_url = os.environ.get('CHROME_EXTENSION_URL', default_ext) or default_ext
        opera_url = os.environ.get('OPERA_EXTENSION_URL', default_ext) or default_ext
        return jsonify({
            "success": True,
            "chromeExtensionUrl": chrome_url,
            "operaExtensionUrl": opera_url,
        })

    @app.route('/api/site/validate', methods=['POST', 'OPTIONS'])
    def site_validate():
        """Валидация кода с бота, создание сессии сайта. Код не помечается used — можно использовать и в расширении."""
        if request.method == 'OPTIONS':
            return jsonify({'status': 'ok'})
        try:
            data = request.get_json() or {}
            code = (data.get('code') or '').strip().upper()
            if not code:
                return jsonify({"success": False, "error": "Код не указан"}), 400
            from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
            conn = get_db_connection()
            cur = get_db_cursor()
            with db_lock:
                cur.execute(
                    "SELECT chat_id, user_id, expires_at, used FROM extension_links WHERE code = %s",
                    (code,)
                )
                row = cur.fetchone()
            if not row:
                return jsonify({"success": False, "error": "Неверный код"}), 400
            chat_id = row.get('chat_id') if isinstance(row, dict) else row[0]
            user_id = row.get('user_id') if isinstance(row, dict) else row[1]
            expires_at = row.get('expires_at') if isinstance(row, dict) else row[2]
            if isinstance(expires_at, str):
                from dateutil import parser
                expires_at = parser.parse(expires_at.replace('Z', '+00:00'))
            if expires_at.tzinfo is None:
                expires_at = pytz.UTC.localize(expires_at)
            if expires_at <= datetime.now(pytz.UTC):
                return jsonify({"success": False, "error": "Код истёк"}), 400
            name = "Профиль"
            try:
                tg_chat = bot.get_chat(chat_id)
                if tg_chat.type in ('group', 'supergroup') and getattr(tg_chat, 'title', None):
                    name = tg_chat.title
                elif getattr(tg_chat, 'first_name', None):
                    name = tg_chat.first_name or name
                elif getattr(tg_chat, 'username', None):
                    name = "@" + tg_chat.username
            except Exception as e:
                logger.warning(f"[SITE API] Не удалось получить имя чата: {e}")
            with db_lock:
                cur.execute("SELECT COUNT(*) FROM movies WHERE chat_id = %s", (chat_id,))
                cnt = cur.fetchone()
                movies_count = (cnt.get('count') if isinstance(cnt, dict) else cnt[0]) or 0
            has_data = movies_count > 0
            token = secrets.token_urlsafe(32)
            session_expires = datetime.now(pytz.UTC) + timedelta(days=30)
            with db_lock:
                cur.execute(
                    "INSERT INTO site_sessions (token, chat_id, user_id, name, expires_at) VALUES (%s, %s, %s, %s, %s)",
                    (token, chat_id, user_id, name, session_expires)
                )
                conn.commit()
            is_personal = (chat_id or 0) > 0
            return jsonify({
                "success": True,
                "token": token,
                "chat_id": chat_id,
                "name": name,
                "has_data": has_data,
                "is_personal": is_personal,
            })
        except Exception as e:
            logger.error(f"[SITE API] validate: {e}", exc_info=True)
            return jsonify({"success": False, "error": "Ошибка сервера"}), 500

    @app.route('/api/site/me', methods=['GET', 'OPTIONS'])
    def site_me():
        if request.method == 'OPTIONS':
            return jsonify({'status': 'ok'})
        chat_id = _site_token_to_chat_id()
        if chat_id is None:
            return jsonify({"success": False, "error": "Не авторизован"}), 401
        from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
        conn = get_db_connection()
        cur = get_db_cursor()
        with db_lock:
            cur.execute(
                "SELECT name FROM site_sessions WHERE chat_id = %s AND expires_at > %s ORDER BY created_at DESC LIMIT 1",
                (chat_id, datetime.now(pytz.UTC))
            )
            row = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM movies WHERE chat_id = %s", (chat_id,))
            cnt = cur.fetchone()
        name = (row.get('name') if isinstance(row, dict) else row[0]) if row else "Профиль"
        movies_count = (cnt.get('count') if isinstance(cnt, dict) else cnt[0]) or 0
        is_personal = (chat_id or 0) > 0
        return jsonify({
            "success": True,
            "name": name,
            "has_data": movies_count > 0,
            "chat_id": chat_id,
            "is_personal": is_personal,
        })

    @app.route('/api/site/plans', methods=['GET', 'OPTIONS'])
    def site_plans():
        if request.method == 'OPTIONS':
            return jsonify({'status': 'ok'})
        chat_id = _site_token_to_chat_id()
        if chat_id is None:
            return jsonify({"success": False, "error": "Не авторизован"}), 401
        from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
        conn = get_db_connection()
        cur = get_db_cursor()
        now_utc = datetime.now(pytz.UTC)
        with db_lock:
            cur.execute("""
                SELECT p.id, p.plan_type, p.plan_datetime, p.film_id, COALESCE(m.title, 'Без названия') as title, m.kp_id, m.is_series
                FROM plans p
                LEFT JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.chat_id = %s AND p.plan_datetime >= %s
                ORDER BY p.plan_datetime
                LIMIT 100
            """, (chat_id, now_utc))
            rows = cur.fetchall()
        home_plans = []
        cinema_plans = []
        for r in rows:
            rec = {
                "id": r.get('id') if isinstance(r, dict) else r[0],
                "plan_type": r.get('plan_type') if isinstance(r, dict) else r[1],
                "plan_datetime": (r.get('plan_datetime') or r[2]).isoformat() if r.get('plan_datetime') or (len(r) > 2 and r[2]) else None,
                "film_id": r.get('film_id') if isinstance(r, dict) else r[3],
                "title": r.get('title') if isinstance(r, dict) else r[4],
                "kp_id": r.get('kp_id') if isinstance(r, dict) else r[5],
                "is_series": bool(r.get('is_series') if isinstance(r, dict) else (r[6] if len(r) > 6 else 0)),
            }
            if rec["plan_type"] == "home":
                home_plans.append(rec)
            else:
                cinema_plans.append(rec)
        return jsonify({"success": True, "home": home_plans, "cinema": cinema_plans})

    @app.route('/api/site/unwatched', methods=['GET', 'OPTIONS'])
    def site_unwatched():
        if request.method == 'OPTIONS':
            return jsonify({'status': 'ok'})
        chat_id = _site_token_to_chat_id()
        if chat_id is None:
            return jsonify({"success": False, "error": "Не авторизован"}), 401
        from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
        conn = get_db_connection()
        cur = get_db_cursor()
        with db_lock:
            cur.execute("""
                SELECT id, kp_id, title, year, is_series, online_link FROM movies
                WHERE chat_id = %s AND watched = 0
                ORDER BY id DESC
                LIMIT 200
            """, (chat_id,))
            rows = cur.fetchall()
        items = []
        for r in rows:
            online_link = r.get('online_link') if isinstance(r, dict) else (r[5] if len(r) > 5 else None)
            items.append({
                "film_id": r.get('id') if isinstance(r, dict) else r[0],
                "kp_id": str(r.get('kp_id') if isinstance(r, dict) else r[1]),
                "title": r.get('title') if isinstance(r, dict) else r[2],
                "year": r.get('year') if isinstance(r, dict) else r[3],
                "is_series": bool(r.get('is_series') if isinstance(r, dict) else (r[4] if len(r) > 4 else 0)),
                "online_link": (online_link or '').strip() or None,
                "description": None,
                "rating_kp": None,
            })
        return jsonify({"success": True, "items": items})

    @app.route('/api/site/series', methods=['GET', 'OPTIONS'])
    def site_series():
        if request.method == 'OPTIONS':
            return jsonify({'status': 'ok'})
        chat_id = _site_token_to_chat_id()
        if chat_id is None:
            return jsonify({"success": False, "error": "Не авторизован"}), 401
        from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
        conn = get_db_connection()
        cur = get_db_cursor()
        user_id_for_series = chat_id
        with db_lock:
            try:
                cur.execute("""
                    SELECT 
                        m.id AS film_id,
                        m.kp_id,
                        m.title,
                        COALESCE(m.is_ongoing, FALSE) AS is_ongoing,
                        COUNT(st.id) AS watched_episodes_count,
                        BOOL_OR(ss.subscribed = TRUE) AS has_subscription,
                        m.online_link
                    FROM movies m
                    LEFT JOIN series_tracking st 
                        ON st.film_id = m.id AND st.chat_id = %s AND st.user_id = %s AND st.watched = TRUE
                    LEFT JOIN series_subscriptions ss 
                        ON ss.film_id = m.id AND ss.chat_id = %s AND ss.user_id = %s AND ss.subscribed = TRUE
                    WHERE m.chat_id = %s AND m.is_series = 1
                    GROUP BY m.id, m.kp_id, m.title, m.is_ongoing, m.online_link
                    LIMIT 100
                """, (chat_id, user_id_for_series, chat_id, user_id_for_series, chat_id))
                rows = cur.fetchall()
            except Exception as e:
                if 'is_ongoing' in str(e).lower() or 'column' in str(e).lower():
                    cur.execute("""
                        SELECT m.id, m.kp_id, m.title, m.online_link FROM movies m
                        WHERE m.chat_id = %s AND m.is_series = 1
                        ORDER BY m.title LIMIT 100
                    """, (chat_id,))
                    rows = cur.fetchall()
                    series_list = []
                    for r in rows:
                        film_id = r.get('id') if isinstance(r, dict) else r[0]
                        kp_id = str(r.get('kp_id') if isinstance(r, dict) else r[1])
                        title = r.get('title') if isinstance(r, dict) else r[2]
                        with db_lock:
                            cur.execute("""
                                SELECT season_number, episode_number FROM series_tracking
                                WHERE chat_id = %s AND film_id = %s AND watched = TRUE
                                ORDER BY season_number DESC, episode_number DESC LIMIT 1
                            """, (chat_id, film_id))
                            last = cur.fetchone()
                        progress = None
                        if last:
                            s = last.get('season_number') if isinstance(last, dict) else last[0]
                            e = last.get('episode_number') if isinstance(last, dict) else last[1]
                            progress = f"S{s} • E{e}"
                        online_link = (r.get('online_link') if isinstance(r, dict) else (r[3] if len(r) > 3 else None)) or ''
                        series_list.append({
                            "film_id": film_id, "kp_id": kp_id, "title": title, "progress": progress,
                            "online_link": (online_link or '').strip() or None
                        })
                    return jsonify({"success": True, "items": series_list})
                raise
        series_list = []
        for r in rows:
            film_id = r.get('film_id') if isinstance(r, dict) else r[0]
            kp_id = str(r.get('kp_id') if isinstance(r, dict) else r[1])
            title = r.get('title') if isinstance(r, dict) else r[2]
            is_ongoing = bool(r.get('is_ongoing') if isinstance(r, dict) else (r[3] if len(r) > 3 else False))
            watched_count = r.get('watched_episodes_count') if isinstance(r, dict) else (r[4] if len(r) > 4 else 0)
            has_subscription = bool(r.get('has_subscription') if isinstance(r, dict) else (r[5] if len(r) > 5 else False))
            with db_lock:
                cur.execute("""
                    SELECT season_number, episode_number FROM series_tracking
                    WHERE chat_id = %s AND film_id = %s AND watched = TRUE
                    ORDER BY season_number DESC, episode_number DESC LIMIT 1
                """, (chat_id, film_id))
                last = cur.fetchone()
            progress = None
            if last:
                s = last.get('season_number') if isinstance(last, dict) else last[0]
                e = last.get('episode_number') if isinstance(last, dict) else last[1]
                progress = f"S{s} • E{e}"
            online_link_raw = r.get('online_link') if isinstance(r, dict) else (r[6] if len(r) > 6 else None)
            series_list.append({
                "film_id": film_id,
                "kp_id": kp_id,
                "title": title,
                "progress": progress,
                "is_ongoing": is_ongoing,
                "watched_count": int(watched_count or 0),
                "has_subscription": has_subscription,
                "online_link": (online_link_raw or '').strip() or None,
            })
        def get_sort_priority(item):
            io = item.get('is_ongoing') or False
            hs = item.get('has_subscription') or False
            wc = item.get('watched_count') or 0
            started = wc > 0
            if started:
                if io and hs: return 1
                if io and not hs: return 2
                if not io and hs: return 3
                return 4
            if io and hs: return 5
            if io and not hs: return 6
            if not io and hs: return 7
            return 8
        series_list.sort(key=get_sort_priority)
        return jsonify({"success": True, "items": series_list})

    @app.route('/api/site/ratings', methods=['GET', 'OPTIONS'])
    def site_ratings():
        if request.method == 'OPTIONS':
            return jsonify({'status': 'ok'})
        chat_id = _site_token_to_chat_id()
        if chat_id is None:
            return jsonify({"success": False, "error": "Не авторизован"}), 401
        from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
        conn = get_db_connection()
        cur = get_db_cursor()
        with db_lock:
            cur.execute("""
                SELECT r.rating, COALESCE(r.kp_id, m.kp_id) as kp_id, m.title, m.year, m.id as film_id,
                    (SELECT s.username FROM stats s
                     WHERE s.chat_id = r.chat_id AND s.user_id = r.user_id
                       AND s.username IS NOT NULL AND s.username != ''
                     ORDER BY s.id DESC LIMIT 1) as rater_username
                FROM ratings r
                JOIN movies m ON r.film_id = m.id AND r.chat_id = m.chat_id
                WHERE r.chat_id = %s
                ORDER BY r.id DESC
                LIMIT 50
            """, (chat_id,))
            rows = cur.fetchall()
        items = []
        for r in rows:
            rater_username = r.get('rater_username') if isinstance(r, dict) else (r[5] if len(r) > 5 else None)
            if rater_username and not rater_username.startswith('@'):
                rater_username = '@' + rater_username
            items.append({
                "rating": r.get('rating') if isinstance(r, dict) else r[0],
                "kp_id": str(r.get('kp_id') or '') if isinstance(r, dict) else str(r[1] or ''),
                "title": r.get('title') if isinstance(r, dict) else r[2],
                "year": r.get('year') if isinstance(r, dict) else r[3],
                "film_id": r.get('film_id') if isinstance(r, dict) else r[4],
                "rater_username": rater_username or None,
                "description": None,
                "rating_kp": None,
            })
        return jsonify({"success": True, "items": items})

    logger.info(f"[WEB APP] ===== FLASK ПРИЛОЖЕНИЕ СОЗДАНО =====")
    logger.info(f"[WEB APP] Зарегистрированные роуты: {[str(rule) for rule in app.url_map.iter_rules()]}")
    logger.info(f"[WEB APP] Возвращаем app: {app}")

    return app

# Запуск приложения (критично для Railway — запускаем здесь)
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))  # Railway сам подставит PORT
    logger.info(f"[WEB APP] Запуск Flask на host=0.0.0.0, port={port}")
    app = create_web_app(None)  # bot=None для теста, в Railway передаётся настоящий bot
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)