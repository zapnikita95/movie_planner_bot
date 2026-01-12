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
                            logger.info(f"[YOOKASSA] Создана новая подписка {subscription_id} (объединенный платеж)")
                            
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
                            logger.info(f"[YOOKASSA] Создана новая подписка 'Все режимы' {subscription_id}")
                            
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
                                logger.info(f"[YOOKASSA] Подписка {subscription_id} продлена")
                                
                                # payment_method_id будет обновлен позже, если карта сохранена
                                
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
                                    logger.info(f"[YOOKASSA] Создана новая подписка {subscription_id}")
                                    
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
                                logger.info(f"[YOOKASSA] Создана новая подписка {subscription_id}")
                                
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
                                'all': 'Все режимы'
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
                    send_successful_payment_notification(
                        chat_id=chat_id,
                        subscription_id=subscription_id,
                        subscription_type=subscription_type,
                        plan_type=plan_type,
                        period_type=period_type
                    )
                    logger.info(f"[YOOKASSA] Уведомление отправлено один раз для подписки {subscription_id}")


                            
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
                            logger.info(f"[YOOKASSA] Создана подписка {subscription_id} для уже обработанного платежа")
                            
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
                                    'all': 'Все режимы'
                                }
                                tariff_name = plan_names.get(plan_type, plan_type)
                                
                                text = "Спасибо за покупку! 🎉\n\n"
                                text += f"Ваша новая подписка: <b>{tariff_name}</b>\n\n"
                                text += "Вот какой функционал вам теперь доступен:\n\n"
                                
                                if plan_type == 'notifications':
                                    text += "🔔 <b>Уведомления о сериалах:</b>\n"
                                    text += "• Автоматические уведомления о выходе новых серий\n"
                                    text += "• Настройка времени уведомлений (будни/выходные)\n"
                                    text += "• Персонализированные напоминания для каждого сериала\n"
                                    text += "• Отслеживание прогресса просмотра сезонов\n"
                                elif plan_type == 'recommendations':
                                    text += "🎯 <b>Персональные рекомендации:</b>\n"
                                    text += "• Режим \"По оценкам в базе\" — рекомендации по оценкам фильмов, добавленных в базу чата или группы\n"
                                    text += "• Режим \"Рандом по Кинопоиску\" — случайный фильм из Кинопоиска по фильтрам\n"
                                    text += "• Режим рандомайзера \"По моим оценкам\" — рекомендации по оценкам из Кинопоиска\n"
                                    text += "• Импорт базы из Кинопоиска\n"
                                elif plan_type == 'tickets':
                                    text += "🎫 <b>Билеты в кино:</b>\n"
                                    text += "• Добавление билетов на сеансы и мероприятия\n"
                                    text += "• Настраиваемые уведомления с билетами перед мероприятием\n"
                                else:  # all
                                    text += "📦 <b>Все режимы:</b>\n\n"
                                    text += "🔔 <b>Уведомления о сериалах:</b>\n"
                                    text += "• Автоматические уведомления о выходе новых серий\n"
                                    text += "• Настройка времени уведомлений (будни/выходные)\n"
                                    text += "• Персонализированные напоминания для каждого сериала\n"
                                    text += "• Отслеживание прогресса просмотра сезонов\n\n"
                                    text += "🎯 <b>Персональные рекомендации:</b>\n"
                                    text += "• Режим \"По оценкам в базе\" — рекомендации по оценкам фильмов, добавленных в базу чата или группы\n"
                                    text += "• Режим \"Рандом по Кинопоиску\" — случайный фильм из Кинопоиска по фильтрам\n"
                                    text += "• Режим рандомайзера \"По моим оценкам\" — рекомендации по оценкам из Кинопоиска\n"
                                    text += "• Импорт базы из Кинопоиска\n\n"
                                    text += "🎫 <b>Билеты в кино:</b>\n"
                                    text += "• Добавление билетов на сеансы и мероприятия\n"
                                    text += "• Настраиваемые уведомления с билетами перед мероприятием\n"
                                
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
                                
                                # Формируем описание возможностей
                                features_text = ""
                                if plan_type == 'all':
                                    features_text = "📦 <b>Доступные функции:</b>\n\n"
                                    features_text += "🔔 <b>Уведомления о сериалах:</b>\n"
                                    features_text += "• Автоматические уведомления о выходе новых серий\n"
                                    features_text += "• Настройка времени уведомлений (будни/выходные)\n"
                                    features_text += "• Персонализированные напоминания для каждого сериала\n\n"
                                    features_text += "🎯 <b>Персональные рекомендации:</b>\n"
                                    features_text += "• Режим \"По оценкам в базе\" — рекомендации по оценкам фильмов, добавленных в базу чата или группы\n"
                                    features_text += "• Режим \"Рандом по Кинопоиску\" — случайный фильм из Кинопоиска по фильтрам\n"
                                    features_text += "• Режим рандомайзера \"По моим оценкам\" — рекомендации по оценкам из Кинопоиска\n"
                                    features_text += "• Импорт базы из Кинопоиска\n\n"
                                    features_text += "🎫 <b>Билеты в кино:</b>\n"
                                    features_text += "• Добавление билетов на сеансы и мероприятия\n"
                                    features_text += "• Настраиваемые уведомления с билетами перед мероприятием\n"
                                elif plan_type == 'notifications':
                                    features_text = "🔔 <b>Доступные функции:</b>\n"
                                    features_text += "• Автоматические уведомления о выходе новых серий\n"
                                    features_text += "• Настройка времени уведомлений (будни/выходные)\n"
                                    features_text += "• Персонализированные напоминания для каждого сериала\n"
                                elif plan_type == 'recommendations':
                                    features_text = "🎯 <b>Доступные функции:</b>\n"
                                    features_text += "• Режим \"По оценкам в базе\" — рекомендации по оценкам фильмов, добавленных в базу чата или группы\n"
                                    features_text += "• Режим \"Рандом по Кинопоиску\" — случайный фильм из Кинопоиска по фильтрам\n"
                                    features_text += "• Режим рандомайзера \"По моим оценкам\" — рекомендации по оценкам из Кинопоиска\n"
                                    features_text += "• Импорт базы из Кинопоиска\n"
                                elif plan_type == 'tickets':
                                    features_text = "🎫 <b>Доступные функции:</b>\n"
                                    features_text += "• Добавление билетов на сеансы и мероприятия\n"
                                    features_text += "• Настраиваемые уведомления с билетами перед мероприятием\n"
                                
                                # Определяем название тарифа для группы
                                plan_names = {
                                    'notifications': 'Уведомления о сериалах',
                                    'recommendations': 'Рекомендации',
                                    'tickets': 'Билеты',
                                    'all': 'Все режимы'
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
                                
                                bot.send_message(chat_id, group_text, parse_mode='HTML')
                                
                                # Отправляем в личку
                                private_text = "Спасибо за покупку! 🎉\n\n"
                                private_text += f"Ваша новая подписка: <b>{tariff_name}</b>\n\n"
                                private_text += "Вот какой функционал вам теперь доступен:\n\n"
                                private_text += features_text
                                
                                if group_size:
                                    private_text += f"\n\n👥 Участников в подписке: <b>{members_count if members_count > 0 else active_count}</b> из {group_size}"
                                
                                private_text += "\n"
                                
                                # Добавляем информацию о чеке, если он был создан
                                if check_url:
                                    private_text += f"📄 <b>Чек от самозанятого:</b>\n"
                                    private_text += f"{check_url}\n"
                                    if pdf_url:
                                        private_text += f"\n📥 <a href=\"{pdf_url}\">Скачать PDF</a>\n"
                                
                                private_text += "\nПриятного просмотра!"
                                
                                bot.send_message(user_id, private_text, parse_mode='HTML')
                                logger.info(f"[YOOKASSA] ✅ Сообщения отправлены для группы {chat_id}, user_id {user_id}, subscription_id {subscription_id}")
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
                                        text += "📦 <b>Доступные функции:</b>\n\n"
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
                                        text += "• Настраиваемые уведомления с билетами перед мероприятием\n"
                                    
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
                                    
                                    # Формируем описание возможностей
                                    features_text = ""
                                    if plan_type == 'all':
                                        features_text = "📦 <b>Доступные функции:</b>\n\n"
                                        features_text += "🔔 <b>Уведомления о сериалах:</b>\n"
                                        features_text += "• Автоматические уведомления о выходе новых серий\n"
                                        features_text += "• Настройка времени уведомлений (будни/выходные)\n"
                                        features_text += "• Персонализированные напоминания для каждого сериала\n\n"
                                        features_text += "🎯 <b>Персональные рекомендации:</b>\n"
                                        features_text += "• Режим \"По оценкам в базе\" — рекомендации по оценкам фильмов, добавленных в базу чата или группы\n"
                                        features_text += "• Режим \"Рандом по Кинопоиску\" — случайный фильм из Кинопоиска по фильтрам\n"
                                        features_text += "• Режим рандомайзера \"По моим оценкам\" — рекомендации по оценкам из Кинопоиска\n"
                                        features_text += "• Импорт базы из Кинопоиска\n\n"
                                        features_text += "🎫 <b>Билеты в кино:</b>\n"
                                        features_text += "• Добавление билетов на сеансы и мероприятия\n"
                                        features_text += "• Настраиваемые уведомления с билетами перед мероприятием\n"
                                    elif plan_type == 'notifications':
                                        features_text = "🔔 <b>Доступные функции:</b>\n"
                                        features_text += "• Автоматические уведомления о выходе новых серий\n"
                                        features_text += "• Настройка времени уведомлений (будни/выходные)\n"
                                        features_text += "• Персонализированные напоминания для каждого сериала\n"
                                    elif plan_type == 'recommendations':
                                        features_text = "🎯 <b>Доступные функции:</b>\n"
                                        features_text += "• Режим \"По оценкам в базе\" — рекомендации по оценкам фильмов, добавленных в базу чата или группы\n"
                                        features_text += "• Режим \"Рандом по Кинопоиску\" — случайный фильм из Кинопоиска по фильтрам\n"
                                        features_text += "• Режим рандомайзера \"По моим оценкам\" — рекомендации по оценкам из Кинопоиска\n"
                                        features_text += "• Импорт базы из Кинопоиска\n"
                                    elif plan_type == 'tickets':
                                        features_text = "🎫 <b>Доступные функции:</b>\n"
                                        features_text += "• Добавление билетов на сеансы и мероприятия\n"
                                        features_text += "• Настраиваемые уведомления с билетами перед мероприятием\n"
                                    
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