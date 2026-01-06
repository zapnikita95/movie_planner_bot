"""
Flask приложение для webhook
"""
from flask import Flask, request, jsonify, abort
import logging
import telebot
import os
import sys
import time
# Импорт yookassa удален, используется moviebot.api.yookassa_api
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла (для локальной разработки)
# В Railway переменные окружения уже доступны через os.getenv()
load_dotenv()

# КРИТИЧНО: Настраиваем логирование Flask так, чтобы оно не конфликтовало с основным
# Используем тот же root logger, что и в main.py
logger = logging.getLogger(__name__)

# Отключаем логирование Werkzeug (Flask по умолчанию), чтобы не перехватывало stdout
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.WARNING)  # Только WARNING и выше, чтобы не засорять логи

# Отключаем логирование Flask
flask_logger = logging.getLogger('flask')
flask_logger.setLevel(logging.WARNING)

app = Flask(__name__)

# Отключаем логирование Flask встроенным способом
app.logger.disabled = True

# Добавляем логирование при создании приложения
logger.info("[WEB APP] Flask приложение создано")

# Глобальное логирование всех запросов - ПРИНУДИТЕЛЬНОЕ
@app.before_request
def log_all_requests():
    # ПРИНУДИТЕЛЬНОЕ ЛОГИРОВАНИЕ - ДОЛЖНО СРАБАТЫВАТЬ ВСЕГДА
    import sys
    print("=" * 80, file=sys.stdout, flush=True)
    print("=== НОВЫЙ ЗАПРОС В FLASK ===", file=sys.stdout, flush=True)
    print(f"Path: {request.path}, Method: {request.method}, IP: {request.remote_addr}", file=sys.stdout, flush=True)
    
    logger.info("=" * 80)
    logger.info("=== НОВЫЙ ЗАПРОС В FLASK ===")
    logger.info(f"Path: {request.path}, Method: {request.method}, IP: {request.remote_addr}")
    logger.info(f"Content-Type: {request.headers.get('content-type')}")
    if request.method == 'POST':
        try:
            data_length = len(request.get_data())
            logger.info(f"Data length: {data_length} bytes")
            if data_length > 0:
                data_preview = request.get_data(as_text=True)[:200]
                logger.info(f"Data preview: {data_preview}...")
        except Exception as e:
            logger.info(f"Data preview: (не удалось прочитать: {e})")
    logger.info("=" * 80)

# Проверяем переменные окружения при старте приложения
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

def create_web_app(bot_instance):
    """Создает Flask приложение с webhook обработчиками"""
    # Получаем ID бота для исключения из подсчета участников
    try:
        bot_info = bot_instance.get_me()
        BOT_ID = bot_info.id
        logger.info(f"[WEB APP] ID бота: {BOT_ID}")
    except Exception as e:
        logger.warning(f"[WEB APP] Не удалось получить ID бота: {e}")
        BOT_ID = None
    
    @app.route('/webhook', methods=['POST', 'GET'])
    def webhook():
        # ПРИНУДИТЕЛЬНОЕ ЛОГИРОВАНИЕ В САМОМ НАЧАЛЕ - И PRINT И LOGGER
        import sys
        print("=" * 80, file=sys.stdout, flush=True)
        print("=== WEBHOOK РОУТ СРАБОТАЛ! Запрос получен ===", file=sys.stdout, flush=True)
        print(f"Method: {request.method}", file=sys.stdout, flush=True)
        print(f"IP: {request.remote_addr}", file=sys.stdout, flush=True)
        
        # НЕ ЧИТАЕМ request.get_data() здесь - это может вызвать проблемы
        print("[WEBHOOK] Шаг 1: Логирование базовой информации", flush=True)
        try:
            logger.info("=" * 80)
            logger.info("=== WEBHOOK РОУТ СРАБОТАЛ! Запрос получен ===")
            logger.info(f"Method: {request.method}")
            logger.info(f"IP: {request.remote_addr}")
            logger.info(f"Path: {request.path}")
            logger.info(f"Content-Type: {request.headers.get('content-type')}")
            logger.info("=" * 80)
        except Exception as e:
            print(f"[WEBHOOK] ОШИБКА в logger: {e}", flush=True)
        
        print(f"[WEBHOOK] Шаг 2: Проверка метода: {request.method}", flush=True)
        if request.method == 'GET':
            print("[WEBHOOK] GET запрос - возвращаем 200", flush=True)
            try:
                logger.info("[WEBHOOK] GET запрос - возвращаем 200")
            except:
                pass
            return "OK", 200
        
        # Логируем POST запросы
        print("[WEBHOOK] Шаг 3: POST запрос получен - продолжаем обработку", flush=True)
        try:
            logger.info(f"[WEBHOOK] POST запрос получен")
        except:
            pass
        
        print("[WEBHOOK] Шаг 4: Получаем content-type", flush=True)
        content_type = request.headers.get('content-type')
        print(f"[WEBHOOK] Шаг 5: Content-Type проверка: '{content_type}'", flush=True)
        try:
            logger.info(f"[WEBHOOK] Content-Type: '{content_type}'")
        except:
            pass
        
        if content_type == 'application/json':
            print("[WEBHOOK] Content-Type правильный, обрабатываем JSON", flush=True)
            try:
                json_string = request.get_data(as_text=True)
                print(f"[WEBHOOK] JSON получен, размер: {len(json_string)} байт", flush=True)
                logger.info(f"[WEBHOOK] JSON получен, размер: {len(json_string)} байт")
                print(f"[WEBHOOK] JSON preview (первые 300 символов): {json_string[:300]}...", flush=True)
                logger.info(f"[WEBHOOK] JSON preview (первые 300 символов): {json_string[:300]}...")
            except Exception as e:
                print(f"[WEBHOOK] ОШИБКА при чтении данных: {e}", flush=True)
                logger.error(f"[WEBHOOK] ОШИБКА при чтении данных: {e}", exc_info=True)
                return '', 200
            
            try:
                print("[WEBHOOK] Начинаем парсинг JSON в Update", flush=True)
                update = telebot.types.Update.de_json(json_string)
                update_id = update.update_id if hasattr(update, 'update_id') else 'N/A'
                print(f"[WEBHOOK] Update распарсен успешно: update_id={update_id}", flush=True)
                logger.info(f"[WEBHOOK] Update распарсен успешно: update_id={update_id}")
                logger.info(f"[WEBHOOK] Тип update: {type(update)}")
                logger.info(f"[WEBHOOK] Update имеет message: {hasattr(update, 'message') and update.message is not None}")
                
                # КРИТИЧНО: Проверяем наличие successful_payment на уровне update
                if hasattr(update, 'message') and update.message and hasattr(update.message, 'successful_payment') and update.message.successful_payment:
                    logger.info(f"[WEBHOOK] ⭐⭐⭐ ОБНАРУЖЕН successful_payment НА УРОВНЕ UPDATE! ⭐⭐⭐")
                    logger.info(f"[WEBHOOK] successful_payment.currency={update.message.successful_payment.currency}")
                    logger.info(f"[WEBHOOK] successful_payment.total_amount={update.message.successful_payment.total_amount}")
                    logger.info(f"[WEBHOOK] successful_payment.invoice_payload={update.message.successful_payment.invoice_payload}")
                
                # Проверяем наличие pre_checkout_query (хотя для Stars не должен прийти)
                if hasattr(update, 'pre_checkout_query') and update.pre_checkout_query:
                    logger.info(f"[WEBHOOK] ⚠️ PRE CHECKOUT QUERY пришел! (хотя для Stars не должен)")
                    logger.info(f"[WEBHOOK] pre_checkout_query.currency={update.pre_checkout_query.currency}")
                    logger.info(f"[WEBHOOK] pre_checkout_query.invoice_payload={update.pre_checkout_query.invoice_payload}")
                
                # Логируем информацию о реплае для отладки
                if update.message:
                    logger.info(f"[WEBHOOK] Update.message.content_type={update.message.content_type if hasattr(update.message, 'content_type') else 'НЕТ'}")
                    logger.info(f"[WEBHOOK] Update.message.text='{update.message.text[:200] if update.message.text else None}'")
                    logger.info(f"[WEBHOOK] Update.message.from_user.id={update.message.from_user.id if update.message.from_user else None}")
                    
                    # КРИТИЧНО: Логируем successful_payment если есть
                    if hasattr(update.message, 'successful_payment') and update.message.successful_payment:
                        logger.info(f"[WEBHOOK] ⭐⭐⭐ ОБНАРУЖЕН successful_payment! ⭐⭐⭐")
                        logger.info(f"[WEBHOOK] successful_payment.currency={update.message.successful_payment.currency}")
                        logger.info(f"[WEBHOOK] successful_payment.total_amount={update.message.successful_payment.total_amount}")
                        logger.info(f"[WEBHOOK] successful_payment.invoice_payload={update.message.successful_payment.invoice_payload}")
                        logger.info(f"[WEBHOOK] successful_payment.telegram_payment_charge_id={getattr(update.message.successful_payment, 'telegram_payment_charge_id', 'N/A')}")
                    
                    # Проверяем наличие web_app_data
                    if hasattr(update.message, 'web_app_data') and update.message.web_app_data:
                        logger.info("🔍 [WEBHOOK] ⚠️⚠️⚠️ ОБНАРУЖЕН web_app_data! ⚠️⚠️⚠️")
                        logger.info(f"[WEBHOOK] web_app_data.data={update.message.web_app_data.data if hasattr(update.message.web_app_data, 'data') else 'НЕТ'}")
                        logger.info(f"[WEBHOOK] web_app_data.button_text={update.message.web_app_data.button_text if hasattr(update.message.web_app_data, 'button_text') else 'НЕТ'}")
                    
                    # Проверяем, является ли сообщение командой
                    if update.message.text and update.message.text.startswith('/'):
                        logger.info(f"[WEBHOOK] ⚠️ Обнаружена команда: '{update.message.text}'")
                        # Проверяем entities для команд
                        if hasattr(update.message, 'entities') and update.message.entities:
                            for entity in update.message.entities:
                                logger.info(f"[WEBHOOK] Entity: type={entity.type}, offset={entity.offset}, length={entity.length}")
                
                # Обрабатываем обновление с обработкой ошибок
                print(f"[WEBHOOK] Вызываем bot.process_new_updates для обработки обновления", flush=True)
                logger.info(f"[WEBHOOK] Вызываем bot.process_new_updates для обработки обновления")
                logger.info(f"[WEBHOOK] Update ID: {update.update_id}, type: {type(update)}")
                if hasattr(update, 'message') and update.message:
                    logger.info(f"[WEBHOOK] Message type: {update.message.content_type if hasattr(update.message, 'content_type') else 'unknown'}")
                if hasattr(update, 'callback_query') and update.callback_query:
                    logger.info(f"[WEBHOOK] Callback query data: {update.callback_query.data[:100] if update.callback_query.data else 'None'}")
                
                print(f"[WEBHOOK] Вызываем bot_instance.process_new_updates([update])", flush=True)
                bot_instance.process_new_updates([update])
                print(f"[WEBHOOK] ✅ bot.process_new_updates завершен успешно", flush=True)
                logger.info(f"[WEBHOOK] ✅ bot.process_new_updates завершен успешно")
                return '', 200
            except Exception as e:
                print(f"[WEBHOOK] ❌ ОШИБКА обработки update: {e}", flush=True)
                import traceback
                print(f"[WEBHOOK] Traceback: {traceback.format_exc()}", flush=True)
                logger.error(f"[WEBHOOK] ❌ Ошибка обработки update: {e}", exc_info=True)
                logger.error(f"[WEBHOOK] Traceback: {traceback.format_exc()}")
                # Возвращаем 200, чтобы Telegram не повторял запрос
                return '', 200
        else:
            print(f"[WEBHOOK] Неверный content-type: {content_type}", flush=True)
            logger.warning(f"[WEBHOOK] Неверный content-type: {content_type}")
            return 'Forbidden', 403
    
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
                    logger.warning(f"[YOOKASSA] Объект: {event_json.get('object')}")
                    return jsonify({'status': 'error', 'message': 'Payment ID not found'}), 400
                
                logger.info(f"[YOOKASSA] Платеж успешен: {payment_id}")
                
                # Импортируем функции для обработки платежа
                from moviebot.database.db_operations import get_payment_by_yookassa_id, update_payment_status, create_subscription, add_subscription_member
                from moviebot.api.yookassa_api import get_payment_info
                
                # Получаем платеж из БД
                logger.info(f"[YOOKASSA] Поиск платежа в БД по yookassa_payment_id: {payment_id}")
                payment_data = get_payment_by_yookassa_id(payment_id)
                
                if not payment_data:
                    logger.warning(f"[YOOKASSA] Платеж {payment_id} не найден в БД")
                    logger.warning(f"[YOOKASSA] Это может быть нормально, если платеж был создан в другом экземпляре бота")
                    return jsonify({'status': 'ok', 'message': 'Payment not found in DB'}), 200
                
                logger.info(f"[YOOKASSA] Платеж найден в БД: payment_id={payment_data.get('payment_id')}, user_id={payment_data.get('user_id')}, chat_id={payment_data.get('chat_id')}, status={payment_data.get('status')}")
                
                # Получаем информацию о платеже из ЮKassa (только если не тестовый режим)
                payment = None
                payment_status = None
                if not is_test:
                    try:
                        logger.info(f"[YOOKASSA] Получение информации о платеже из ЮKassa API...")
                        payment = get_payment_info(payment_id)
                        payment_status = payment.status if payment else None
                        logger.info(f"[YOOKASSA] Статус платежа из ЮKassa: {payment_status}")
                    except Exception as e:
                        logger.error(f"[YOOKASSA] Ошибка получения платежа из ЮKassa: {e}", exc_info=True)
                        # В тестовом режиме или при ошибке используем данные из БД
                        payment_status = 'succeeded' if event_json.get('event') == 'payment.succeeded' else 'canceled'
                else:
                    # В тестовом режиме используем статус из события
                    payment_status = 'succeeded' if event_json.get('event') == 'payment.succeeded' else 'canceled'
                
                logger.info(f"[YOOKASSA] Текущий статус в БД: {payment_data.get('status')}, статус из ЮKassa: {payment_status}")
                
                # Обрабатываем успешный платеж, если он еще не был обработан
                # Проверяем, что статус из ЮKassa succeeded и в БД статус не succeeded
                db_status = payment_data.get('status')
                if payment_status == 'succeeded' and db_status != 'succeeded':
                    logger.info(f"[YOOKASSA] Платеж успешен, обновляем статус и создаем/продлеваем подписку")
                    # Обновляем статус платежа
                    update_payment_status(payment_data['payment_id'], 'succeeded')
                    
                    # Создаем подписку
                    if payment and hasattr(payment, 'metadata') and payment.metadata:
                        metadata = payment.metadata
                    elif is_test and event_json.get('object', {}).get('metadata'):
                        # В тестовом режиме берем metadata из тестового уведомления
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
                        # Сохраняем payment_method_id для рекуррентных платежей
                        # Важно: сохраняем только если payment_method.saved == True
                        payment_method_id = None
                        if hasattr(payment, 'payment_method') and payment.payment_method:
                            # Проверяем, что способ оплаты сохранен
                            is_saved = getattr(payment.payment_method, 'saved', False)
                            if is_saved:
                                payment_method_id = getattr(payment.payment_method, 'id', None)
                                logger.info(f"[YOOKASSA] Способ оплаты сохранен, payment_method_id: {payment_method_id}")
                            else:
                                logger.info(f"[YOOKASSA] Способ оплаты не сохранен (saved=False), payment_method_id не будет использован")
                        else:
                            logger.info(f"[YOOKASSA] payment_method отсутствует в платеже")
                    else:
                        amount = float(payment_data['amount'])
                        payment_method_id = None
                    
                    # Определяем telegram_username и group_username из metadata
                    telegram_username = metadata.get('telegram_username')
                    group_username = metadata.get('group_username')
                    
                    # Проверяем, является ли это обновлением существующей подписки
                    upgrade_subscription_id = metadata.get('upgrade_subscription_id')
                    upgrade_from_plan = metadata.get('upgrade_from_plan')
                    
                    # Проверяем, является ли это объединенным платежом
                    is_combined = metadata.get('is_combined', 'false').lower() == 'true'
                    combine_type = metadata.get('combine_type')
                    
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
                                payment_method_id=payment_method_id
                            )
                            logger.info(f"[YOOKASSA] Создана новая подписка {subscription_id} (объединенный платеж)")
                            
                            # Автоматически добавляем оплатившего пользователя в групповую подписку
                            if subscription_id and subscription_type == 'group':
                                try:
                                    add_subscription_member(subscription_id, user_id, telegram_username)
                                    logger.info(f"[YOOKASSA] Оплативший пользователь {user_id} (@{telegram_username}) автоматически добавлен в подписку {subscription_id}")
                                except Exception as add_error:
                                    logger.error(f"[YOOKASSA] Ошибка при автоматическом добавлении оплатившего пользователя: {add_error}", exc_info=True)
                            
                            # Отправляем уведомление об успешном платеже
                            if subscription_id:
                                from moviebot.scheduler import send_successful_payment_notification
                                send_successful_payment_notification(
                                    chat_id=chat_id,
                                    subscription_id=subscription_id,
                                    subscription_type=subscription_type,
                                    plan_type=plan_type,
                                    period_type=period_type
                                )
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
                                payment_method_id=payment_method_id
                            )
                            logger.info(f"[YOOKASSA] Создана новая подписка 'Все режимы' {subscription_id}")
                            
                            # Автоматически добавляем оплатившего пользователя в групповую подписку
                            if subscription_id and subscription_type == 'group':
                                try:
                                    add_subscription_member(subscription_id, user_id, telegram_username)
                                    logger.info(f"[YOOKASSA] Оплативший пользователь {user_id} (@{telegram_username}) автоматически добавлен в подписку {subscription_id}")
                                except Exception as add_error:
                                    logger.error(f"[YOOKASSA] Ошибка при автоматическом добавлении оплатившего пользователя: {add_error}", exc_info=True)
                            
                            # Отправляем уведомление об успешном платеже
                            if subscription_id:
                                from moviebot.scheduler import send_successful_payment_notification
                                send_successful_payment_notification(
                                    chat_id=chat_id,
                                    subscription_id=subscription_id,
                                    subscription_type=subscription_type,
                                    plan_type='all',
                                    period_type=period_type
                                )
                        except Exception as sub_error:
                            logger.error(f"[YOOKASSA] Ошибка при создании новой подписки: {sub_error}", exc_info=True)
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
                                
                                # Обновляем payment_method_id, если он был сохранен
                                if payment_method_id:
                                    from moviebot.database.db_connection import get_db_connection, db_lock
                                    conn_update = get_db_connection()
                                    cursor_update = conn_update.cursor()
                                    with db_lock:
                                        cursor_update.execute("""
                                            UPDATE subscriptions 
                                            SET payment_method_id = %s, updated_at = NOW()
                                            WHERE id = %s
                                        """, (payment_method_id, upgrade_sub_id))
                                        conn_update.commit()
                                    logger.info(f"[YOOKASSA] payment_method_id {payment_method_id} обновлен в подписке {upgrade_sub_id}")
                                
                                subscription_id = upgrade_sub_id
                                logger.info(f"[YOOKASSA] Подписка {upgrade_sub_id} обновлена: {upgrade_from_plan} -> {plan_type}, цена: {new_price}₽")
                                
                                # Отправляем уведомление об успешном платеже
                                from moviebot.scheduler import send_successful_payment_notification
                                send_successful_payment_notification(
                                    chat_id=chat_id,
                                    subscription_id=subscription_id,
                                    subscription_type=subscription_type,
                                    plan_type=plan_type,
                                    period_type=period_type_upgrade
                                )
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
                                
                                # Обновляем payment_method_id в подписке, если он был сохранен
                                if payment_method_id:
                                    from moviebot.database.db_connection import get_db_connection, db_lock
                                    conn_update = get_db_connection()
                                    cursor_update = conn_update.cursor()
                                    with db_lock:
                                        cursor_update.execute("""
                                            UPDATE subscriptions 
                                            SET payment_method_id = %s, updated_at = NOW()
                                            WHERE id = %s
                                        """, (payment_method_id, subscription_id))
                                        conn_update.commit()
                                    logger.info(f"[YOOKASSA] payment_method_id {payment_method_id} обновлен в подписке {subscription_id}")
                                
                                # Отправляем уведомление об успешном платеже
                                from moviebot.scheduler import send_successful_payment_notification
                                send_successful_payment_notification(
                                    chat_id=chat_id,
                                    subscription_id=subscription_id,
                                    subscription_type=subscription_type,
                                    plan_type=plan_type,
                                    period_type=period_type
                                )
                            else:
                                # Параметры не совпадают - создаем новую подписку
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
                                        payment_method_id=payment_method_id
                                    )
                                    logger.info(f"[YOOKASSA] Создана новая подписка {subscription_id}")
                                    
                                    # Автоматически добавляем оплатившего пользователя в групповую подписку
                                    if subscription_id and subscription_type == 'group':
                                        try:
                                            add_subscription_member(subscription_id, user_id, telegram_username)
                                            logger.info(f"[YOOKASSA] Оплативший пользователь {user_id} (@{telegram_username}) автоматически добавлен в подписку {subscription_id}")
                                        except Exception as add_error:
                                            logger.error(f"[YOOKASSA] Ошибка при автоматическом добавлении оплатившего пользователя: {add_error}", exc_info=True)
                                    
                                    # Отправляем уведомление об успешном платеже
                                    if subscription_id:
                                        from moviebot.scheduler import send_successful_payment_notification
                                        send_successful_payment_notification(
                                            chat_id=chat_id,
                                            subscription_id=subscription_id,
                                            subscription_type=subscription_type,
                                            plan_type=plan_type,
                                            period_type=period_type
                                        )
                                except Exception as sub_error:
                                    logger.error(f"[YOOKASSA] Ошибка при создании новой подписки: {sub_error}", exc_info=True)
                                    subscription_id = None
                        else:
                            # Нет активной подписки - создаем новую
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
                                    payment_method_id=payment_method_id
                                )
                                logger.info(f"[YOOKASSA] Создана новая подписка {subscription_id}")
                                
                                # Автоматически добавляем оплатившего пользователя в групповую подписку
                                if subscription_id and subscription_type == 'group':
                                    try:
                                        add_subscription_member(subscription_id, user_id, telegram_username)
                                        logger.info(f"[YOOKASSA] Оплативший пользователь {user_id} (@{telegram_username}) автоматически добавлен в подписку {subscription_id}")
                                    except Exception as add_error:
                                        logger.error(f"[YOOKASSA] Ошибка при автоматическом добавлении оплатившего пользователя: {add_error}", exc_info=True)
                                
                                # Отправляем уведомление об успешном платеже
                                if subscription_id:
                                    from moviebot.scheduler import send_successful_payment_notification
                                    send_successful_payment_notification(
                                        chat_id=chat_id,
                                        subscription_id=subscription_id,
                                        subscription_type=subscription_type,
                                        plan_type=plan_type,
                                        period_type=period_type
                                    )
                            except Exception as sub_error:
                                logger.error(f"[YOOKASSA] Ошибка при создании новой подписки: {sub_error}", exc_info=True)
                                subscription_id = None
                    
                    # Сохраняем payment_method_id в платеж
                    if payment_method_id:
                        from moviebot.database.db_connection import get_db_connection, db_lock
                        conn = get_db_connection()
                        cursor = conn.cursor()
                        with db_lock:
                            cursor.execute("""
                                UPDATE payments 
                                SET payment_method_id = %s, updated_at = NOW()
                                WHERE payment_id = %s
                            """, (payment_method_id, payment_data['payment_id']))
                            conn.commit()
                        logger.info(f"[YOOKASSA] payment_method_id {payment_method_id} сохранен в платеж {payment_data['payment_id']}")
                    
                    # Обновляем платеж с subscription_id (даже если subscription_id = None)
                    logger.info(f"[YOOKASSA] Обновляем статус платежа на 'succeeded' с subscription_id={subscription_id}")
                    try:
                        update_payment_status(payment_data['payment_id'], 'succeeded', subscription_id)
                    except Exception as update_error:
                        logger.error(f"[YOOKASSA] Ошибка при обновлении статуса платежа: {update_error}", exc_info=True)
                    
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
                    
                    # Отправляем уведомление об успешном платеже
                    if subscription_id:
                        try:
                            from moviebot.scheduler import send_successful_payment_notification
                            send_successful_payment_notification(
                                chat_id=chat_id,
                                subscription_id=subscription_id,
                                subscription_type=subscription_type,
                                plan_type=plan_type,
                                period_type=period_type
                            )
                            logger.info(f"[YOOKASSA] Уведомление об успешном платеже отправлено для подписки {subscription_id}")
                        except Exception as notify_error:
                            logger.error(f"[YOOKASSA] Ошибка отправки уведомления об успешном платеже: {notify_error}", exc_info=True)
                    
                    # Старый блок отправки подробного уведомления удален - теперь используется send_successful_payment_notification
                    
                    # Обработка групповых подписок
                    if subscription_type == 'group':
                        # Для групповой подписки отправляем в группу и в личку
                        try:
                            from moviebot.database.db_operations import get_active_group_users, get_subscription_members
                            
                            # Получаем участников подписки
                            # get_subscription_members возвращает dict {user_id: username}
                            members_dict = get_subscription_members(subscription_id, BOT_ID) if subscription_id else {}
                            members_count = len(members_dict) if members_dict else 0
                            
                            # Проверяем количество участников
                            active_users = get_active_group_users(chat_id, bot_id=BOT_ID)
                            active_count = len(active_users) if active_users else 0
                            
                            # Формируем список участников, получивших доступ
                            members_list = ""
                            if members_dict:
                                members_list = "\n\n👥 <b>Участники с доступом:</b>\n"
                                for member_user_id, member_username in list(members_dict.items())[:20]:  # Ограничение на 20 участников
                                    members_list += f"• @{member_username or f'user_{member_user_id}'}\n"
                                if len(members_dict) > 20:
                                    members_list += f"• ... и еще {len(members_dict) - 20} участников\n"
                            elif active_users and active_count <= (group_size or active_count):
                                # Если участники не выбраны, но активных пользователей не больше лимита, показываем всех
                                members_list = "\n\n👥 <b>Участники с доступом:</b>\n"
                                for member_user_id, member_username in list(active_users.items())[:20]:
                                    members_list += f"• @{member_username or f'user_{member_user_id}'}\n"
                                if active_count > 20:
                                    members_list += f"• ... и еще {active_count - 20} участников\n"
                            
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
                            
                            # Сообщение для группы
                            group_text = "Спасибо за покупку! 🎉\n\n"
                            group_text += f"Ваша новая подписка: <b>{tariff_name}</b>\n\n"
                            group_text += "Вот какой функционал вам теперь доступен:\n\n"
                            group_text += features_text
                            group_text += members_list
                            
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
                            
                            # Формируем клавиатуру для предложения добавить участников
                            markup = None
                            
                            # Проверяем, есть ли место в подписке и есть ли потенциальные участники
                            if subscription_id and group_size and members_count < group_size and active_users:
                                # Исключаем бота и оплатившего из списка потенциальных участников
                                potential_members = {}
                                for member_user_id, member_username in active_users.items():
                                    # Пропускаем бота
                                    if BOT_ID and member_user_id == BOT_ID:
                                        continue
                                    # Пропускаем оплатившего (он уже добавлен)
                                    if member_user_id == user_id:
                                        continue
                                    # Пропускаем уже добавленных участников
                                    if members_dict and member_user_id in members_dict:
                                        continue
                                    potential_members[member_user_id] = member_username
                                
                                # Если есть потенциальные участники
                                if potential_members:
                                    markup = InlineKeyboardMarkup(row_width=1)
                                    
                                    # Если потенциальных участников ровно 1 и есть место - предлагаем кнопку с его ником
                                    if len(potential_members) == 1 and members_count + 1 <= group_size:
                                        member_user_id = list(potential_members.keys())[0]
                                        member_username = potential_members[member_user_id]
                                        member_display = f"@{member_username}" if member_username else f"user_{member_user_id}"
                                        markup.add(InlineKeyboardButton(
                                            f"➕ Добавить {member_display}",
                                            callback_data=f"payment:add_member:{member_user_id}:{subscription_id}"
                                        ))
                                        group_text += f"\n\n💡 Хотите добавить {member_display} в подписку?"
                                    # Если потенциальных участников несколько - предлагаем кнопку "Выбрать участников"
                                    elif len(potential_members) > 1:
                                        markup.add(InlineKeyboardButton(
                                            "👥 Выбрать участников",
                                            callback_data=f"payment:select_members:{subscription_id}"
                                        ))
                                        group_text += f"\n\n💡 В группе есть еще участники, которых можно добавить в подписку."
                            
                            # Если есть ограничение по количеству участников и активных пользователей больше
                            if group_size and active_count > group_size and members_count < group_size and not markup:
                                group_text += f"\n\n⚠️ <b>Внимание!</b>\n"
                                group_text += f"В группе <b>{active_count}</b> активных участников, а подписка рассчитана на <b>{group_size}</b>.\n"
                                group_text += f"Выберите участников для подписки:"
                                
                                markup = InlineKeyboardMarkup(row_width=1)
                                markup.add(InlineKeyboardButton("👥 Выбрать участников", callback_data=f"payment:select_members:{subscription_id}"))
                            
                            # Отправляем сообщение
                            try:
                                if markup:
                                    result = bot_instance.send_message(chat_id, group_text, reply_markup=markup, parse_mode='HTML')
                                    logger.info(f"[YOOKASSA] ✅ Сообщение с кнопкой отправлено в группу {chat_id}, message_id={result.message_id if result else 'N/A'}")
                                else:
                                    result = bot_instance.send_message(chat_id, group_text, parse_mode='HTML')
                                    logger.info(f"[YOOKASSA] ✅ Сообщение успешно отправлено в группу {chat_id}, user_id {user_id}, subscription_id {subscription_id}, message_id={result.message_id if result else 'N/A'}")
                            except Exception as send_error:
                                logger.error(f"[YOOKASSA] ❌ Ошибка отправки сообщения в группу: {send_error}", exc_info=True)
                                logger.warning(f"[YOOKASSA] Продолжаем выполнение несмотря на ошибку отправки сообщения в группу")
                            
                            # Отправляем такое же сообщение в личку тому, кто оплатил
                            private_text = "Спасибо за покупку! 🎉\n\n"
                            private_text += f"Ваша новая подписка: <b>{tariff_name}</b>\n\n"
                            private_text += "Вот какой функционал вам теперь доступен:\n\n"
                            private_text += features_text
                            private_text += members_list
                            
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
                            
                            try:
                                result = bot_instance.send_message(user_id, private_text, parse_mode='HTML')
                                logger.info(f"[YOOKASSA] ✅ Сообщение об успешной оплате отправлено в личку пользователю {user_id}, message_id={result.message_id if result else 'N/A'}")
                            except Exception as send_error:
                                logger.error(f"[YOOKASSA] ❌ Ошибка отправки сообщения в личку пользователю {user_id}: {send_error}", exc_info=True)
                                logger.warning(f"[YOOKASSA] Продолжаем выполнение несмотря на ошибку отправки сообщения в личку")
                            
                            logger.info(f"[YOOKASSA] Подписка создана для группы {chat_id}, user_id {user_id}, subscription_id {subscription_id}")
                        except Exception as e:
                            logger.error(f"[YOOKASSA] Ошибка отправки уведомления: {e}", exc_info=True)
                            # Не прерываем выполнение, просто логируем ошибку
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
                                payment_method_id=payment_method_id
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
                                
                                bot_instance.send_message(target_chat_id, text, parse_mode='HTML')
                                logger.info(f"[YOOKASSA] ✅ Сообщение отправлено для пользователя {user_id}, subscription_id {subscription_id}")
                                
                            elif subscription_type == 'group':
                                from moviebot.database.db_operations import get_active_group_users, get_subscription_members
                                
                                members_dict = get_subscription_members(subscription_id, BOT_ID) if subscription_id else {}
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
                                
                                bot_instance.send_message(chat_id, group_text, parse_mode='HTML')
                                
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
                                
                                bot_instance.send_message(user_id, private_text, parse_mode='HTML')
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
                                    
                                    bot_instance.send_message(chat_id, text, parse_mode='HTML')
                                    logger.info(f"[YOOKASSA] ✅ Сообщение отправлено для пользователя {user_id}, subscription_id {subscription_id_from_payment}")
                                
                                elif subscription_type == 'group':
                                    from moviebot.database.db_operations import get_active_group_users, get_subscription_members
                                    
                                    members_dict = get_subscription_members(subscription_id_from_payment, BOT_ID) if subscription_id_from_payment else {}
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
                                    
                                    bot_instance.send_message(chat_id, group_text, parse_mode='HTML')
                                    
                                    # Отправляем в личку
                                    private_text = "Спасибо за подписку! Вот какой функционал вам теперь доступен:\n\n"
                                    private_text += "👥 <b>Групповая подписка активирована</b>\n\n"
                                    private_text += features_text
                                    
                                    if group_size:
                                        private_text += f"\n\n👥 Участников в подписке: <b>{members_count if members_count > 0 else active_count}</b> из {group_size}"
                                    
                                    private_text += "\n\nСпасибо за покупку! 🎉"
                                    
                                    bot_instance.send_message(user_id, private_text, parse_mode='HTML')
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
    
    @app.route('/health', methods=['GET'])
    def health():
        """Улучшенный health check endpoint с проверкой всех компонентов"""
        logger.info("[HEALTH] Health check запрос получен")
        
        try:
            # Пытаемся получить статус от watchdog, если он доступен
            try:
                from moviebot.utils.watchdog import get_watchdog
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
    
    logger.info(f"[WEB APP] ===== FLASK ПРИЛОЖЕНИЕ СОЗДАНО =====")
    logger.info(f"[WEB APP] Зарегистрированные роуты: {[str(rule) for rule in app.url_map.iter_rules()]}")
    logger.info(f"[WEB APP] Возвращаем app: {app}")
    return app


