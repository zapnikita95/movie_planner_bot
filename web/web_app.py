"""
Flask приложение для webhook
"""
from flask import Flask, request, jsonify, abort
import logging
import telebot
import os
import sys
import time
from yookassa import Configuration, Payment

logger = logging.getLogger(__name__)

app = Flask(__name__)

def create_web_app(bot_instance):
    """Создает Flask приложение с webhook обработчиками"""
    
    @app.route('/webhook', methods=['POST'])
    def webhook():
        logger.info("=" * 80)
        logger.info("[WEBHOOK] ===== ПОЛУЧЕН ЗАПРОС =====")
        if request.headers.get('content-type') == 'application/json':
            json_string = request.get_data().decode('utf-8')
            logger.info(f"[WEBHOOK] Размер JSON: {len(json_string)} байт")
            # Проверяем, есть ли web_app_data в сыром JSON
            if 'web_app_data' in json_string.lower():
                logger.info("🔍 [WEBHOOK] ⚠️⚠️⚠️ В JSON ЕСТЬ 'web_app_data'! ⚠️⚠️⚠️")
            # Логируем первые 1000 символов JSON для отладки
            logger.info(f"[WEBHOOK] JSON (первые 1000 символов): {json_string[:1000]}")
            update = telebot.types.Update.de_json(json_string)
            logger.info(f"[WEBHOOK] Тип update: {type(update)}")
            logger.info(f"[WEBHOOK] Update имеет message: {hasattr(update, 'message') and update.message is not None}")
            
            # Логируем информацию о реплае для отладки
            if update.message:
                logger.info(f"[WEBHOOK] Update.message.content_type={update.message.content_type if hasattr(update.message, 'content_type') else 'НЕТ'}")
                logger.info(f"[WEBHOOK] Update.message.text='{update.message.text[:200] if update.message.text else None}'")
                logger.info(f"[WEBHOOK] Update.message.from_user.id={update.message.from_user.id if update.message.from_user else None}")
            
            bot_instance.process_new_updates([update])
            return '', 200
        else:
            logger.warning("[WEBHOOK] Неверный content-type")
            abort(400)
    
    def process_yookassa_notification(event_json, is_test=False):
        """Обрабатывает уведомление от ЮKassa (можно вызывать из webhook или теста)"""
        try:
            logger.info(f"[YOOKASSA] Обработка события: {event_json.get('event')} (тест: {is_test})")
            
            if event_json.get('event') == 'payment.succeeded':
                payment_id = event_json.get('object', {}).get('id')
                if payment_id:
                    logger.info(f"[YOOKASSA] Платеж успешен: {payment_id}")
                    
                    # Импортируем функции для обработки платежа
                    from database.db_operations import get_payment_by_yookassa_id, update_payment_status, create_subscription
                    from config.settings import YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY
                    
                    # Инициализируем ЮKassa для получения информации о платеже
                    Configuration.account_id = YOOKASSA_SHOP_ID
                    Configuration.secret_key = YOOKASSA_SECRET_KEY
                    
                    # Получаем платеж из БД
                    logger.info(f"[YOOKASSA] Поиск платежа в БД по yookassa_payment_id: {payment_id}")
                    payment_data = get_payment_by_yookassa_id(payment_id)
                    
                    if not payment_data:
                        logger.warning(f"[YOOKASSA] Платеж {payment_id} не найден в БД")
                        logger.warning(f"[YOOKASSA] Это может быть нормально, если платеж был создан в другом экземпляре бота")
                        return jsonify({'status': 'ok', 'message': 'Payment not found in DB'}), 200
                    
                    logger.info(f"[YOOKASSA] Платеж найден в БД: {payment_data}")
                    
                    # Получаем информацию о платеже из ЮKassa (только если не тестовый режим)
                    payment = None
                    payment_status = None
                    if not is_test:
                        try:
                            payment = Payment.find_one(payment_id)
                            payment_status = payment.status if payment else None
                        except Exception as e:
                            logger.error(f"[YOOKASSA] Ошибка получения платежа из ЮKassa: {e}")
                            # В тестовом режиме или при ошибке используем данные из БД
                            payment_status = 'succeeded' if event_json.get('event') == 'payment.succeeded' else 'canceled'
                    else:
                        # В тестовом режиме используем статус из события
                        payment_status = 'succeeded' if event_json.get('event') == 'payment.succeeded' else 'canceled'
                    
                    if payment_status == 'succeeded' and payment_data.get('status') != 'succeeded':
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
                        else:
                            amount = float(payment_data['amount'])
                        
                        # Определяем telegram_username и group_username из metadata
                        telegram_username = metadata.get('telegram_username')
                        group_username = metadata.get('group_username')
                        
                        # Проверяем, есть ли уже активная подписка с такими же параметрами
                        from database.db_operations import get_active_subscription, renew_subscription
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
                                    group_username=group_username,
                                    group_size=group_size
                                )
                                logger.info(f"[YOOKASSA] Создана новая подписка {subscription_id}")
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
                                group_username=group_username,
                                group_size=group_size
                            )
                            logger.info(f"[YOOKASSA] Создана новая подписка {subscription_id}")
                        
                        # Обновляем платеж с subscription_id
                        logger.info(f"[YOOKASSA] Обновляем статус платежа на 'succeeded' с subscription_id={subscription_id}")
                        update_payment_status(payment_data['payment_id'], 'succeeded', subscription_id)
                        
                        # Отправляем подробное уведомление пользователю
                        try:
                            from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                            
                            # Определяем, куда отправлять сообщение
                            target_chat_id = chat_id
                            logger.info(f"[YOOKASSA] Подготовка отправки уведомления в chat_id={target_chat_id}, user_id={user_id}")
                            
                            # Формируем описание функций в зависимости от типа подписки
                            if subscription_type == 'personal':
                                text = "✅ <b>Платеж успешно обработан!</b>\n\n"
                                text += "👤 <b>Личная подписка активирована</b>\n\n"
                                
                                if plan_type == 'notifications':
                                    text += "🔔 <b>Доступные функции:</b>\n"
                                    text += "• Автоматические уведомления о выходе новых серий\n"
                                    text += "• Настройка времени уведомлений (будни/выходные)\n"
                                    text += "• Персонализированные напоминания для каждого сериала\n"
                                    text += "• Отслеживание прогресса просмотра сезонов\n"
                                elif plan_type == 'recommendations':
                                    text += "🎯 <b>Доступные функции:</b>\n"
                                    text += "• Режим рандомайзера \"по моим оценкам\" (9-10)\n"
                                    text += "• Режим \"по групповым оценкам\" (9-10)\n"
                                    text += "• Режим \"рандом по кинопоиск\" с фильтрами\n"
                                    text += "• Импорт базы оценок из Кинопоиска\n"
                                    text += "• Умные рекомендации на основе ваших предпочтений\n"
                                elif plan_type == 'tickets':
                                    text += "🎫 <b>Доступные функции:</b>\n"
                                    text += "• Добавление билетов на сеансы в кино\n"
                                    text += "• Хранение билетов в базе бота\n"
                                    text += "• Уведомления с билетами перед сеансом\n"
                                    text += "• Настройка времени напоминания о билетах\n"
                                else:  # all
                                    text += "📦 <b>Доступные функции:</b>\n\n"
                                    text += "🔔 <b>Уведомления о сериалах:</b>\n"
                                    text += "• Автоматические уведомления о выходе новых серий\n"
                                    text += "• Настройка времени уведомлений\n\n"
                                    text += "🎯 <b>Персональные рекомендации:</b>\n"
                                    text += "• Режим \"по моим оценкам\"\n"
                                    text += "• Режим \"по групповым оценкам\"\n"
                                    text += "• Режим \"рандом по кинопоиск\"\n"
                                    text += "• Импорт базы из Кинопоиска\n\n"
                                    text += "🎫 <b>Билеты на мероприятия:</b>\n"
                                    text += "• Добавление билетов на сеансы\n"
                                    text += "• Уведомления с билетами перед сеансом\n"
                                
                                text += "\n\nСпасибо за покупку! 🎉"
                                
                                # Отправляем сообщение для личной подписки
                                logger.info(f"[YOOKASSA] Отправка сообщения об успешной оплате в chat_id={target_chat_id}")
                                try:
                                    bot_instance.send_message(target_chat_id, text, parse_mode='HTML')
                                    logger.info(f"[YOOKASSA] ✅ Сообщение успешно отправлено для пользователя {user_id}, chat_id {target_chat_id}, subscription_id {subscription_id}")
                                except Exception as send_error:
                                    logger.error(f"[YOOKASSA] ❌ Ошибка отправки сообщения: {send_error}", exc_info=True)
                                    raise
                                
                            elif subscription_type == 'group':
                                # Для групповой подписки отправляем в группу
                                from database.db_operations import get_active_group_users, get_subscription_members
                                
                                text = "✅ <b>Платеж успешно обработан!</b>\n\n"
                                text += "👥 <b>Групповая подписка активирована</b>\n\n"
                                
                                if plan_type == 'all':
                                    text += "📦 <b>Доступные функции:</b>\n\n"
                                    text += "🔔 <b>Уведомления о сериалах:</b>\n"
                                    text += "• Автоматические уведомления о выходе новых серий\n"
                                    text += "• Настройка времени уведомлений\n\n"
                                    text += "🎯 <b>Персональные рекомендации:</b>\n"
                                    text += "• Режим \"по моим оценкам\"\n"
                                    text += "• Режим \"по групповым оценкам\"\n"
                                    text += "• Режим \"рандом по кинопоиск\"\n"
                                    text += "• Импорт базы из Кинопоиска\n\n"
                                    text += "🎫 <b>Билеты на мероприятия:</b>\n"
                                    text += "• Добавление билетов на сеансы\n"
                                    text += "• Уведомления с билетами перед сеансом\n"
                                
                                # Проверяем количество участников
                                active_users = get_active_group_users(chat_id)
                                active_count = len(active_users) if active_users else 0
                                
                                # Получаем участников подписки
                                # get_subscription_members возвращает dict {user_id: username}
                                members_dict = get_subscription_members(subscription_id) if subscription_id else {}
                                members_count = len(members_dict) if members_dict else 0
                                
                                # Если есть ограничение по количеству участников и активных пользователей больше
                                if group_size and active_count > group_size and members_count < group_size:
                                    text += f"\n\n⚠️ <b>Внимание!</b>\n"
                                    text += f"В группе <b>{active_count}</b> активных участников, а подписка рассчитана на <b>{group_size}</b>.\n"
                                    text += f"Выберите участников для подписки:"
                                    
                                    markup = InlineKeyboardMarkup(row_width=1)
                                    markup.add(InlineKeyboardButton("👥 Выбрать участников", callback_data=f"payment:select_members:{subscription_id}"))
                                    bot_instance.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
                                else:
                                    text += f"\n\n👥 Участников в подписке: <b>{members_count if members_count > 0 else active_count}</b>"
                                    if group_size:
                                        text += f" из {group_size}"
                                    text += "\n\nСпасибо за покупку! 🎉"
                                    logger.info(f"[YOOKASSA] Отправка сообщения об успешной оплате в группу chat_id={chat_id}")
                                    try:
                                        bot_instance.send_message(chat_id, text, parse_mode='HTML')
                                        logger.info(f"[YOOKASSA] ✅ Сообщение успешно отправлено в группу {chat_id}, user_id {user_id}, subscription_id {subscription_id}")
                                    except Exception as send_error:
                                        logger.error(f"[YOOKASSA] ❌ Ошибка отправки сообщения в группу: {send_error}", exc_info=True)
                                        raise
                                
                                logger.info(f"[YOOKASSA] Подписка создана для группы {chat_id}, user_id {user_id}, subscription_id {subscription_id}")
                            
                        except Exception as e:
                            logger.error(f"[YOOKASSA] Ошибка отправки уведомления: {e}", exc_info=True)
            
            elif event_json.get('event') == 'payment.canceled':
                payment_id = event_json.get('object', {}).get('id')
                if payment_id:
                    logger.info(f"[YOOKASSA] Платеж отменен: {payment_id}")
                    from database.db_operations import get_payment_by_yookassa_id, update_payment_status
                    payment_data = get_payment_by_yookassa_id(payment_id)
                    if payment_data:
                        update_payment_status(payment_data['payment_id'], 'canceled')
            
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
        logger.info("[HEALTH] Health check запрос получен")
        return jsonify({'status': 'ok', 'bot': 'running'}), 200
    
    @app.route('/yookassa/webhook', methods=['POST', 'GET'])
    def yookassa_webhook():
        """Обработчик webhook от ЮKassa"""
        if request.method == 'GET':
            # Для проверки доступности endpoint
            return jsonify({'status': 'ok', 'message': 'YooKassa webhook endpoint is active'}), 200
        
        try:
            logger.info("=" * 80)
            logger.info("[YOOKASSA WEBHOOK] ===== ПОЛУЧЕН ЗАПРОС ОТ ЮKASSA =====")
            logger.info(f"[YOOKASSA WEBHOOK] Headers: {dict(request.headers)}")
            logger.info(f"[YOOKASSA WEBHOOK] Content-Type: {request.content_type}")
            
            event_json = request.json
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
            from database.db_operations import get_payment_by_yookassa_id
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
    
    return app


