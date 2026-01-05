"""
Отдельные handlers для каждого состояния пользователя
Каждый handler обрабатывает конкретный тип состояния с поддержкой:
- Реплаев на сообщения бота
- Личных чатов (можно отвечать без реплая)
- Обработки ошибок с кнопками
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from moviebot.bot.bot_init import bot as bot_instance
from moviebot.bot.bot_init import BOT_ID

logger = logging.getLogger(__name__)


def _process_promo_success(message, state, promo_code, discounted_price, message_text, promocode_id, user_id, chat_id):
    """Внутренняя функция для обработки успешного применения промокода"""
    try:
        sub_type = state['sub_type']
        plan_type = state['plan_type']
        period_type = state['period_type']
        group_size = state.get('group_size')
        payment_id = state.get('payment_id', '')
        
        # Обновляем цену в состоянии платежа
        from moviebot.states import user_payment_state
        if user_id in user_payment_state:
            payment_state = user_payment_state[user_id]
            payment_state['price'] = discounted_price
            payment_state['promocode_id'] = promocode_id
            payment_state['promocode'] = promo_code
            payment_state['original_price'] = state['original_price']
            
            if 'payment_data' in payment_state:
                payment_state['payment_data']['amount'] = discounted_price
        
        # Формируем сообщение с обновленной ценой
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
        
        from moviebot.bot.callbacks.payment_callbacks import rubles_to_stars
        stars_amount = rubles_to_stars(discounted_price)
        
        text_result = f"✅ {message_text}\n\n"
        text_result += f"💳 <b>Оплата подписки</b>\n\n"
        text_result += f"📋 <b>Выбранный тариф:</b>\n"
        if sub_type == 'personal':
            text_result += f"👤 Личная подписка\n"
        else:
            text_result += f"👥 Групповая подписка (на {group_size} участников)\n"
        text_result += f"{plan_name}\n"
        text_result += f"⏰ Период: {period_name}\n"
        text_result += f"💰 Сумма: <b>{state['original_price']}₽</b> → <b>{discounted_price}₽</b>\n\n"
        text_result += "Нажмите кнопку ниже для перехода к оплате:"
        
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Создаем платеж YooKassa с учетом скидки
        from moviebot.config import YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY
        import os
        import uuid as uuid_module
        
        if YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY:
            from yookassa import Configuration, Payment
            Configuration.account_id = YOOKASSA_SHOP_ID.strip()
            Configuration.secret_key = YOOKASSA_SECRET_KEY.strip()
            
            new_payment_id = str(uuid_module.uuid4())
            return_url = os.getenv('YOOKASSA_RETURN_URL', 'tg://resolve?domain=movie_planner_bot')
            description = f"{subscription_type_name}: {plan_name}, период: {period_name}"
            
            metadata = {
                "user_id": str(user_id),
                "chat_id": str(chat_id),
                "subscription_type": sub_type,
                "plan_type": plan_type,
                "period_type": period_type,
                "payment_id": new_payment_id,
                "promocode": promo_code
            }
            if group_size:
                metadata["group_size"] = str(group_size)
            
            try:
                payment = Payment.create({
                    "amount": {"value": f"{discounted_price:.2f}", "currency": "RUB"},
                    "confirmation": {"type": "redirect", "return_url": return_url},
                    "capture": True,
                    "description": description,
                    "metadata": metadata
                })
                
                from moviebot.database.db_operations import save_payment
                save_payment(
                    payment_id=new_payment_id,
                    yookassa_payment_id=payment.id,
                    user_id=user_id,
                    chat_id=chat_id,
                    subscription_type=sub_type,
                    plan_type=plan_type,
                    period_type=period_type,
                    group_size=group_size,
                    amount=discounted_price,
                    status='pending'
                )
                
                confirmation_url = payment.confirmation.confirmation_url
                markup.add(InlineKeyboardButton("💳 Оплатить картой/ЮMoney", url=confirmation_url))
                payment_id = new_payment_id
            except Exception as e:
                logger.error(f"[PROMO HANDLER] Ошибка создания платежа YooKassa: {e}", exc_info=True)
        
        # Обновляем состояние платежа
        if user_id in user_payment_state:
            payment_state = user_payment_state[user_id]
            payment_state['payment_id'] = payment_id
            payment_state['price'] = discounted_price
            payment_state['promocode_id'] = promocode_id
            payment_state['promocode'] = promo_code
            payment_state['original_price'] = state['original_price']
            
            if 'payment_data' in payment_state:
                payment_state['payment_data']['payment_id'] = payment_id
                payment_state['payment_data']['amount'] = discounted_price
            else:
                payment_state['payment_data'] = {
                    'payment_id': payment_id,
                    'amount': discounted_price,
                    'sub_type': sub_type,
                    'plan_type': plan_type,
                    'period_type': period_type,
                    'group_size': group_size,
                    'chat_id': chat_id
                }
        
        # Добавляем кнопки оплаты
        payment_id_short = payment_id[:8] if len(payment_id) > 8 else payment_id
        callback_data_stars = f"payment:pay_stars:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}:{payment_id_short}"
        markup.add(InlineKeyboardButton(f"⭐ Оплатить звездами Telegram ({stars_amount}⭐)", callback_data=callback_data_stars))
        callback_data_promo = f"payment:promo:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}:{payment_id_short}:{discounted_price}"
        markup.add(InlineKeyboardButton("🏷️ Промокод", callback_data=callback_data_promo))
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:subscribe:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}" if group_size else f"payment:subscribe:{sub_type}:{plan_type}:{period_type}"))
        
        try:
            bot_instance.reply_to(message, text_result, reply_markup=markup, parse_mode='HTML')
        except Exception as e:
            logger.error(f"[PROMO HANDLER] Ошибка отправки: {e}", exc_info=True)
            bot_instance.send_message(chat_id, text_result, reply_markup=markup, parse_mode='HTML')
        
        # Удаляем состояние промокода
        from moviebot.states import user_promo_state
        if user_id in user_promo_state:
            del user_promo_state[user_id]
    except Exception as e:
        logger.error(f"[PROMO HANDLER] Ошибка обработки успешного промокода: {e}", exc_info=True)
        raise


def should_process_message(message, state, prompt_message_id=None, require_reply_in_groups=True):
    """
    Определяет, нужно ли обрабатывать сообщение.
    
    Args:
        message: Сообщение от пользователя
        state: Состояние пользователя
        prompt_message_id: ID сообщения-промпта (если есть)
        require_reply_in_groups: Требовать ли реплай в групповых чатах
    
    Returns:
        tuple: (should_process: bool, is_reply: bool, is_private: bool)
    """
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Определяем тип чата
    try:
        chat_info = bot_instance.get_chat(chat_id)
        is_private = chat_info.type == 'private'
    except:
        is_private = chat_id > 0  # Положительные ID обычно личные чаты
    
    # Проверяем реплай
    is_reply = (message.reply_to_message and 
                message.reply_to_message.from_user and 
                message.reply_to_message.from_user.id == BOT_ID)
    
    # В личных чатах можно отвечать без реплая
    if is_private:
        # Если есть промпт, проверяем, что это ответ на него (или просто текст в личном чате)
        if prompt_message_id:
            if is_reply and message.reply_to_message.message_id == prompt_message_id:
                return True, True, True
            elif not is_reply:
                # В личном чате можно отвечать без реплая
                return True, False, True
            else:
                # Реплай на другое сообщение - не обрабатываем
                return False, False, True
        else:
            # Нет промпта - обрабатываем любой текст в личном чате
            return True, is_reply, True
    else:
        # В групповых чатах требуется реплай
        if require_reply_in_groups:
            if not is_reply:
                return False, False, False
            
            if prompt_message_id:
                if message.reply_to_message.message_id == prompt_message_id:
                    return True, True, False
                else:
                    return False, True, False
            else:
                return True, True, False
        else:
            # Если не требуется реплай в группах
            return True, is_reply, False


def send_error_message(message, error_text, prompt_message_id=None, state=None, back_callback=None):
    """
    Отправляет сообщение об ошибке с кнопками для повторной попытки.
    
    Args:
        message: Сообщение от пользователя
        error_text: Текст ошибки
        prompt_message_id: ID сообщения-промпта (для повторной отправки)
        state: Состояние пользователя (для кнопки "Назад")
        back_callback: Callback для кнопки "Назад"
    """
    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    
    markup = InlineKeyboardMarkup(row_width=1)
    
    # Кнопка "Попробовать снова" - отправляет промпт еще раз
    if prompt_message_id:
        markup.add(InlineKeyboardButton("🔄 Попробовать снова", callback_data=f"retry_prompt:{prompt_message_id}"))
    
    # Кнопка "Вернуться назад" - возврат на предыдущий шаг
    if back_callback:
        markup.add(InlineKeyboardButton("◀️ Вернуться назад", callback_data=back_callback))
    elif state:
        # Пытаемся определить callback для возврата назад из состояния
        if 'back_callback' in state:
            markup.add(InlineKeyboardButton("◀️ Вернуться назад", callback_data=state['back_callback']))
    
    # Кнопка "Отмена" - отменяет текущее действие
    markup.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_action"))
    
    try:
        bot_instance.reply_to(message, error_text, reply_markup=markup, parse_mode='HTML')
    except Exception as e:
        logger.error(f"[ERROR MESSAGE] Ошибка отправки сообщения об ошибке: {e}", exc_info=True)
        try:
            bot_instance.send_message(message.chat.id, error_text, reply_markup=markup, parse_mode='HTML')
        except Exception as e2:
            logger.error(f"[ERROR MESSAGE] Критическая ошибка отправки: {e2}", exc_info=True)


def handle_retry_prompt_callback(call):
    """Обработчик кнопки 'Попробовать снова' - отправляет промпт еще раз"""
    try:
        prompt_message_id = int(call.data.split(":")[1])
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # Получаем оригинальное сообщение-промпт
        try:
            prompt_message = bot_instance.forward_message(chat_id, chat_id, prompt_message_id)
            # Или просто отправляем новое сообщение с тем же текстом
            # Для этого нужно сохранять текст промпта в состоянии
            bot_instance.answer_callback_query(call.id, "Промпт отправлен повторно")
        except Exception as e:
            logger.error(f"[RETRY PROMPT] Ошибка: {e}", exc_info=True)
            bot_instance.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
    except Exception as e:
        logger.error(f"[RETRY PROMPT] Ошибка обработки: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except:
            pass


def handle_cancel_action_callback(call):
    """Обработчик кнопки 'Отмена' - очищает состояние пользователя"""
    try:
        user_id = call.from_user.id
        
        # Очищаем все состояния
        from moviebot.states import (
            user_ticket_state, user_search_state, user_import_state,
            user_edit_state, user_settings_state, user_plan_state,
            user_clean_state, user_promo_state, user_promo_admin_state,
            user_cancel_subscription_state, user_refund_state,
            user_unsubscribe_state, user_add_admin_state, user_view_film_state
        )
        
        states_to_clear = [
            user_ticket_state, user_search_state, user_import_state,
            user_edit_state, user_settings_state, user_plan_state,
            user_clean_state, user_promo_state, user_promo_admin_state,
            user_cancel_subscription_state, user_refund_state,
            user_unsubscribe_state, user_add_admin_state, user_view_film_state
        ]
        
        for state_dict in states_to_clear:
            if user_id in state_dict:
                del state_dict[user_id]
        
        bot_instance.answer_callback_query(call.id, "✅ Действие отменено")
        bot_instance.edit_message_text(
            "❌ Действие отменено",
            call.message.chat.id,
            call.message.message_id
        )
    except Exception as e:
        logger.error(f"[CANCEL ACTION] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except:
            pass


# Регистрируем обработчики кнопок
@bot_instance.callback_query_handler(func=lambda call: call.data.startswith("retry_prompt:"))
def retry_prompt_callback(call):
    handle_retry_prompt_callback(call)


@bot_instance.callback_query_handler(func=lambda call: call.data == "cancel_action")
def cancel_action_callback(call):
    handle_cancel_action_callback(call)


# ==================== HANDLER ДЛЯ ОЦЕНОК ====================

def check_rating_message(message):
    """Проверяет, является ли сообщение оценкой (1-10)"""
    if not message.text:
        return False
    
    text_stripped = message.text.strip()
    # Проверяем, является ли сообщение числом от 1 до 10
    is_rating = (len(text_stripped) == 1 and text_stripped.isdigit() and 1 <= int(text_stripped) <= 9) or \
                (len(text_stripped) == 2 and text_stripped == "10")
    
    if not is_rating:
        return False
    
    # Проверяем, есть ли реплай на сообщение с запросом оценки
    if message.reply_to_message:
        reply_text = message.reply_to_message.text or ""
        # Проверяем, содержит ли сообщение запрос на оценку
        rating_prompts = [
            "Оцените фильм",
            "Укажите оценку",
            "Поставьте оценку",
            "Введите оценку",
            "Оценка"
        ]
        if any(prompt.lower() in reply_text.lower() for prompt in rating_prompts):
            return True
        
        # Проверяем rating_messages
        from moviebot.states import rating_messages
        if message.reply_to_message.message_id in rating_messages:
            return True
    
    # В личных чатах можно отправлять оценку без реплая
    try:
        chat_info = bot_instance.get_chat(message.chat.id)
        if chat_info.type == 'private':
            return True
    except:
        if message.chat.id > 0:  # Положительные ID обычно личные чаты
            return True
    
    return False


@bot_instance.message_handler(content_types=['text'], func=check_rating_message)
def handle_rating(message):
    """Обработчик для оценок фильмов"""
    logger.info(f"[RATE HANDLER] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    try:
        user_id = message.from_user.id
        text = message.text.strip()
        rating = int(text)
        
        # Проверяем, есть ли реплай
        is_reply = (message.reply_to_message and 
                   message.reply_to_message.from_user and 
                   message.reply_to_message.from_user.id == BOT_ID)
        
        if is_reply:
            reply_msg_id = message.reply_to_message.message_id
            from moviebot.states import rating_messages
            if reply_msg_id in rating_messages:
                logger.info(f"[RATE HANDLER] Обработка оценки через rating_messages: {rating}")
                from moviebot.bot.handlers.rate import handle_rating_internal
                handle_rating_internal(message, rating)
                return
        
        # Если нет реплая или не найдено в rating_messages, пробуем обработать
        logger.info(f"[RATE HANDLER] Попытка обработки оценки без реплая или вне rating_messages: {rating}")
        from moviebot.bot.handlers.rate import handle_rating_internal
        try:
            handle_rating_internal(message, rating)
        except Exception as e:
            logger.error(f"[RATE HANDLER] Ошибка обработки оценки: {e}", exc_info=True)
            send_error_message(
                message,
                "❌ Не получилось обработать оценку. Пожалуйста, ответьте на сообщение с запросом оценки.",
                prompt_message_id=message.reply_to_message.message_id if message.reply_to_message else None,
                back_callback="back_to_start_menu"
            )
    except Exception as e:
        logger.error(f"[RATE HANDLER] Критическая ошибка: {e}", exc_info=True)
        send_error_message(
            message,
            "❌ Не получилось обработать сообщение",
            prompt_message_id=message.reply_to_message.message_id if message.reply_to_message else None,
            back_callback="back_to_start_menu"
        )


# ==================== HANDLER ДЛЯ ПРОМОКОДОВ ====================

def check_promo_message(message):
    """Проверяет, является ли сообщение промокодом"""
    from moviebot.states import user_promo_state, user_promo_admin_state
    user_id = message.from_user.id
    
    if user_id not in user_promo_state and user_id not in user_promo_admin_state:
        return False
    
    if not message.text or not message.text.strip():
        return False
    
    # Проверяем, есть ли реплай на сообщение с запросом промокода
    if message.reply_to_message:
        reply_text = message.reply_to_message.text or ""
        promo_prompts = [
            "Введите промокод",
            "Укажите промокод",
            "Промокод",
            "введите промокод в ответном сообщении"
        ]
        if any(prompt.lower() in reply_text.lower() for prompt in promo_prompts):
            return True
    
    # В личных чатах можно отправлять промокод без реплая
    try:
        chat_info = bot_instance.get_chat(message.chat.id)
        if chat_info.type == 'private':
            return True
    except:
        if message.chat.id > 0:
            return True
    
    return False


@bot_instance.message_handler(content_types=['text'], func=check_promo_message)
def handle_promo(message):
    """Обработчик для промокодов"""
    logger.info(f"[PROMO HANDLER] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    try:
        from moviebot.states import user_promo_state, user_promo_admin_state
        user_id = message.from_user.id
        chat_id = message.chat.id
        text = message.text.strip()
        
        # Проверяем состояние
        if user_id in user_promo_state:
            state = user_promo_state[user_id]
            prompt_message_id = state.get('prompt_message_id')
            
            # Проверяем, нужно ли обрабатывать
            should_process, is_reply, is_private = should_process_message(
                message, state, prompt_message_id, require_reply_in_groups=True
            )
            
            if not should_process:
                logger.info(f"[PROMO HANDLER] Пропускаем сообщение (не соответствует условиям)")
                return
            
            # Обрабатываем промокод
            promo_code = text.upper()
            logger.info(f"[PROMO HANDLER] Обработка промокода: {promo_code}")
            
            try:
                # Переносим логику из MAIN TEXT HANDLER
                if not promo_code:
                    send_error_message(
                        message,
                        "❌ Промокод не может быть пустым. Введите промокод.",
                        prompt_message_id=prompt_message_id,
                        state=state,
                        back_callback="payment:back_from_promo"
                    )
                    return
                
                # Проверяем, не был ли уже применен этот промокод в текущей сессии платежа
                from moviebot.states import user_payment_state
                if user_id in user_payment_state:
                    payment_state = user_payment_state[user_id]
                    applied_promo = payment_state.get('promocode')
                    applied_promo_id = payment_state.get('promocode_id')
                    
                    if applied_promo or applied_promo_id:
                        logger.warning(f"[PROMO HANDLER] Промокод уже применен: promocode={applied_promo}, promocode_id={applied_promo_id}")
                        error_text = f"❌ Промокод уже применен к этому платежу.\n\n"
                        error_text += "Вы не можете применить промокод повторно."
                        
                        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                        markup = InlineKeyboardMarkup()
                        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:back_from_promo"))
                        
                        bot_instance.reply_to(message, error_text, reply_markup=markup, parse_mode='HTML')
                        return
                
                # Применяем промокод к оригинальной цене
                original_price = state.get('original_price')
                if not original_price:
                    from moviebot.states import user_payment_state
                    if user_id in user_payment_state:
                        payment_state = user_payment_state[user_id]
                        original_price = payment_state.get('original_price', state.get('original_price', 0))
                    else:
                        original_price = state.get('original_price', 0)
                
                logger.info(f"[PROMO HANDLER] Применяем промокод '{promo_code}' к оригинальной цене {original_price}")
                
                # Применяем промокод
                from moviebot.utils.promo import apply_promocode
                success, discounted_price, message_text, promocode_id = apply_promocode(
                    promo_code,
                    original_price,
                    user_id,
                    chat_id
                )
                
                # Проверяем, что итоговая сумма не меньше 0
                if discounted_price < 0:
                    discounted_price = 0
                    logger.warning(f"[PROMO HANDLER] Итоговая сумма после применения промокода меньше 0, установлена в 0")
                
                logger.info(f"[PROMO HANDLER] Результат: success={success}, discounted_price={discounted_price}, message='{message_text}'")
                
                if success:
                    # Промокод применен успешно - обрабатываем результат
                    _process_promo_success(message, state, promo_code, discounted_price, message_text, promocode_id, user_id, chat_id)
                else:
                    # Промокод недействителен
                    error_text = f"❌ {message_text}\n\n"
                    error_text += "Введите другой промокод или оплатите полную стоимость подписки."
                    
                    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                    markup = InlineKeyboardMarkup()
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:back_from_promo"))
                    
                    bot_instance.reply_to(message, error_text, reply_markup=markup)
                    # Не удаляем состояние, чтобы пользователь мог попробовать другой промокод
                    
            except Exception as e:
                logger.error(f"[PROMO HANDLER] Ошибка обработки промокода: {e}", exc_info=True)
                send_error_message(
                    message,
                    "❌ Не получилось обработать промокод. Проверьте правильность ввода.",
                    prompt_message_id=prompt_message_id,
                    state=state,
                    back_callback="payment:back_from_promo"
                )
        
        elif user_id in user_promo_admin_state:
            state = user_promo_admin_state[user_id]
            
            # В личных чатах можно отвечать без реплая
            try:
                chat_info = bot_instance.get_chat(chat_id)
                is_private = chat_info.type == 'private'
            except:
                is_private = chat_id > 0
            
            if not is_private:
                # В группах требуется реплай
                if not message.reply_to_message:
                    return
            
            # Обрабатываем создание промокода
            parts = text.strip().split()
            if len(parts) != 3:
                send_error_message(
                    message,
                    "❌ Неверный формат. Используйте: КОД СКИДКА КОЛИЧЕСТВО\n\nНапример: NEW2026 20% 100",
                    state=state,
                    back_callback="admin:back"
                )
                return
            
            try:
                from moviebot.utils.promo import create_promocode
                code = parts[0].strip()
                discount_input = parts[1].strip()
                total_uses_str = parts[2].strip()
                
                success, result_message = create_promocode(code, discount_input, total_uses_str)
                
                if success:
                    bot_instance.reply_to(message, f"✅ {result_message}")
                else:
                    bot_instance.reply_to(message, f"❌ {result_message}")
                
                del user_promo_admin_state[user_id]
            except Exception as e:
                logger.error(f"[PROMO HANDLER] Ошибка создания промокода: {e}", exc_info=True)
                send_error_message(
                    message,
                    "❌ Не получилось обработать промокод. Проверьте правильность ввода.",
                    state=state,
                    back_callback="admin:back"
                )
    except Exception as e:
        logger.error(f"[PROMO HANDLER] Критическая ошибка: {e}", exc_info=True)
        send_error_message(
            message,
            "❌ Не получилось обработать сообщение",
            back_callback="back_to_start_menu"
        )


# ==================== HANDLER ДЛЯ БИЛЕТОВ ====================

def check_ticket_message(message):
    """Проверяет, является ли сообщение ответом в состоянии билетов"""
    from moviebot.states import user_ticket_state
    user_id = message.from_user.id
    
    if user_id not in user_ticket_state:
        return False
    
    if not message.text or not message.text.strip():
        return False
    
    state = user_ticket_state[user_id]
    step = state.get('step')
    
    # Проверяем, есть ли реплай на сообщение бота
    is_reply = (message.reply_to_message and 
                message.reply_to_message.from_user and 
                message.reply_to_message.from_user.id == BOT_ID)
    
    # В личных чатах можно отвечать без реплая
    try:
        chat_info = bot_instance.get_chat(message.chat.id)
        is_private = chat_info.type == 'private'
    except:
        is_private = message.chat.id > 0
    
    # Для некоторых шагов требуется реплай даже в личных чатах
    if step in ['waiting_new_session', 'waiting_session_time', 'edit_time']:
        if not is_private and not is_reply:
            return False
        if is_private:
            return True  # В личных чатах можно без реплая
    
    if step == 'upload_ticket':
        # Для upload_ticket ожидаются файлы, но можно обработать "готово"
        return message.text.lower().strip() == 'готово'
    
    return True


@bot_instance.message_handler(content_types=['text'], func=check_ticket_message)
def handle_ticket(message):
    """Обработчик для билетов"""
    logger.info(f"[TICKET HANDLER] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    try:
        from moviebot.states import user_ticket_state
        user_id = message.from_user.id
        chat_id = message.chat.id
        text = message.text.strip() if message.text else ""
        
        if user_id not in user_ticket_state:
            return
        
        state = user_ticket_state[user_id]
        step = state.get('step')
        
        try:
            # Обработка билета на мероприятие
            if state.get('type') == 'event':
                if step == 'event_name':
                    event_name = text.strip()
                    if not event_name:
                        send_error_message(
                            message,
                            "❌ Название мероприятия не может быть пустым. Попробуйте еще раз.",
                            state=state,
                            back_callback="back_to_start_menu"
                        )
                        return
                    
                    state['event_name'] = event_name
                    state['step'] = 'event_datetime'
                    
                    bot_instance.reply_to(
                        message,
                        f"✅ Название мероприятия: <b>{event_name}</b>\n\n"
                        "Теперь укажите дату и время мероприятия в ответ на это сообщение.\n"
                        "Формат: 15 января 19:30 или 17.01 15:20",
                        parse_mode='HTML'
                    )
                    return
                
                elif step == 'event_datetime':
                    from moviebot.database.db_operations import get_user_timezone_or_default
                    from moviebot.utils.parsing import parse_session_time
                    import pytz
                    
                    user_tz = get_user_timezone_or_default(user_id)
                    event_dt = parse_session_time(text, user_tz)
                    
                    if not event_dt:
                        send_error_message(
                            message,
                            "❌ Не удалось распознать дату и время. Попробуйте в формате:\n• 15 января 19:30\n• 17.01 15:20",
                            state=state,
                            back_callback="back_to_start_menu"
                        )
                        return
                    
                    state['event_datetime'] = event_dt
                    state['step'] = 'event_file'
                    
                    event_utc = event_dt.astimezone(pytz.utc)
                    state['event_datetime_utc'] = event_utc
                    
                    tz_name = "MSK" if user_tz.zone == 'Europe/Moscow' else "CET" if user_tz.zone == 'Europe/Belgrade' else "UTC"
                    formatted_time = event_dt.strftime('%d.%m.%Y %H:%M')
                    
                    bot_instance.reply_to(
                        message,
                        f"✅ Дата и время: <b>{formatted_time} {tz_name}</b>\n\n"
                        "Теперь отправьте файл или картинку с билетом:",
                        parse_mode='HTML'
                    )
                    return
            
            if step == 'waiting_new_session':
                from moviebot.bot.handlers.series import handle_new_session_input_internal
                handle_new_session_input_internal(message, state)
                return
            
            if step == 'upload_ticket':
                if text.lower().strip() == 'готово':
                    from moviebot.bot.handlers.series import ticket_done_internal
                    ticket_done_internal(message, state)
                    return
                logger.info(f"[TICKET HANDLER] Игнорируем текст в режиме upload_ticket (ожидаются фото/документы)")
                return
            
            if step == 'waiting_session_time':
                from moviebot.bot.handlers.series import handle_edit_ticket_text_internal
                handle_edit_ticket_text_internal(message, state)
                return
            
            if step == 'edit_time':
                plan_id = state.get('plan_id')
                chat_id_state = state.get('chat_id')
                
                if not plan_id:
                    send_error_message(
                        message,
                        "❌ Ошибка: сеанс не найден.",
                        state=state,
                        back_callback="back_to_start_menu"
                    )
                    if user_id in user_ticket_state:
                        del user_ticket_state[user_id]
                    return
                
                from moviebot.utils.parsing import parse_session_time
                from moviebot.database.db_operations import get_user_timezone_or_default
                import pytz
                from moviebot.database.db_connection import db_lock, conn, cursor
                
                user_tz = get_user_timezone_or_default(user_id)
                new_dt = parse_session_time(text, user_tz)
                
                if not new_dt:
                    send_error_message(
                        message,
                        "❌ Не удалось распознать дату и время. Попробуйте еще раз.\nФормат: 18 января 19:30 или 18.01 19:30",
                        state=state,
                        back_callback="back_to_start_menu"
                    )
                    return
                
                if new_dt.tzinfo is None:
                    new_dt_utc = user_tz.localize(new_dt).astimezone(pytz.utc)
                else:
                    new_dt_utc = new_dt.astimezone(pytz.utc)
                
                with db_lock:
                    cursor.execute("UPDATE plans SET plan_datetime = %s WHERE id = %s AND chat_id = %s", (new_dt_utc, plan_id, chat_id_state))
                    conn.commit()
                
                new_dt_local = new_dt_utc.astimezone(user_tz)
                date_str = new_dt_local.strftime('%d.%m.%Y %H:%M')
                
                bot_instance.reply_to(message, f"✅ Время сеанса изменено на {date_str}")
                
                if user_id in user_ticket_state:
                    del user_ticket_state[user_id]
                
                from moviebot.bot.handlers.series import show_cinema_sessions
                show_cinema_sessions(chat_id_state, user_id, None)
                return
                
        except Exception as e:
            logger.error(f"[TICKET HANDLER] Ошибка обработки: {e}", exc_info=True)
            send_error_message(
                message,
                "❌ Не получилось обработать сообщение",
                state=state,
                back_callback="back_to_start_menu"
            )
    except Exception as e:
        logger.error(f"[TICKET HANDLER] Критическая ошибка: {e}", exc_info=True)
        send_error_message(
            message,
            "❌ Не получилось обработать сообщение",
            back_callback="back_to_start_menu"
        )


# ==================== HANDLER ДЛЯ ПОИСКА ====================

def check_search_message(message):
    """Проверяет, является ли сообщение запросом поиска"""
    from moviebot.states import user_search_state
    user_id = message.from_user.id
    
    if user_id not in user_search_state:
        return False
    
    if not message.text or not message.text.strip():
        return False
    
    # В личных чатах можно отвечать без реплая
    try:
        chat_info = bot_instance.get_chat(message.chat.id)
        is_private = chat_info.type == 'private'
    except:
        is_private = message.chat.id > 0
    
    # Проверяем реплай
    is_reply = (message.reply_to_message and 
                message.reply_to_message.from_user and 
                message.reply_to_message.from_user.id == BOT_ID)
    
    state = user_search_state[user_id]
    saved_message_id = state.get('message_id')
    
    if is_private:
        # В личных чатах можно отвечать без реплая
        return True
    else:
        # В группах требуется реплай
        if is_reply and message.reply_to_message.message_id == saved_message_id:
            return True
    
    return False


@bot_instance.message_handler(content_types=['text'], func=check_search_message)
def handle_search(message):
    """Обработчик для поиска"""
    logger.info(f"[SEARCH HANDLER] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    try:
        from moviebot.states import user_search_state
        from moviebot.bot.handlers.series import search_films_with_type
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
        
        user_id = message.from_user.id
        chat_id = message.chat.id
        text = message.text.strip() if message.text else ""
        
        if user_id not in user_search_state:
            return
        
        state = user_search_state[user_id]
        
        if not text:
            return
        
        try:
            query = text.strip()
            search_type = state.get('search_type', 'mixed')
            
            logger.info(f"[SEARCH HANDLER] Поиск по запросу '{query}' от пользователя {user_id}, тип: {search_type}")
            
            films, total_pages = search_films_with_type(query, page=1, search_type=search_type)
            logger.info(f"[SEARCH HANDLER] ✅ Поиск завершен: найдено {len(films) if films else 0} результатов, страниц: {total_pages}")
            
            if not films:
                bot_instance.reply_to(message, f"❌ Ничего не найдено по запросу '{query}'")
                if user_id in user_search_state:
                    del user_search_state[user_id]
                return
            
            results_text = f"🔍 Результаты поиска '{query}':\n\n"
            markup = InlineKeyboardMarkup(row_width=1)
            
            films_to_process = films[:10]
            
            for idx, film in enumerate(films_to_process):
                try:
                    title = film.get('nameRu') or film.get('nameEn') or film.get('title') or "Без названия"
                    year = film.get('year') or film.get('releaseYear') or 'N/A'
                    rating = film.get('ratingKinopoisk') or film.get('rating') or film.get('ratingImdb') or 'N/A'
                    kp_id = film.get('kinopoiskId') or film.get('filmId') or film.get('id')
                    
                    film_type = film.get('type', '').upper() if film.get('type') else 'FILM'
                    is_series = film_type == 'TV_SERIES'
                    
                    if kp_id:
                        type_indicator = "📺" if is_series else "🎬"
                        button_text = f"{type_indicator} {title} ({year})"
                        if len(button_text) > 50:
                            button_text = button_text[:47] + "..."
                        results_text += f"• {type_indicator} <b>{title}</b> ({year})"
                        if rating != 'N/A':
                            results_text += f" ⭐ {rating}"
                        results_text += "\n"
                        markup.add(InlineKeyboardButton(button_text, callback_data=f"add_film_{kp_id}:{film_type}"))
                except Exception as film_e:
                    logger.error(f"[SEARCH HANDLER] Ошибка обработки фильма {idx+1}: {film_e}", exc_info=True)
                    continue
            
            if total_pages > 1:
                pagination_row = []
                query_encoded = query.replace(' ', '_')
                pagination_row.append(InlineKeyboardButton(f"Страница 1/{total_pages}", callback_data="noop"))
                if total_pages > 1:
                    pagination_row.append(InlineKeyboardButton("Далее ▶️", callback_data=f"search_{query_encoded}_2"))
                markup.row(*pagination_row)
            
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            results_text += "\n\n🎬 - фильм\n📺 - сериал"
            
            if len(results_text) > 4096:
                max_length = 4000
                results_text = results_text[:max_length] + "\n\n... (показаны не все результаты)"
            
            try:
                sent_message = bot_instance.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
                logger.info(f"[SEARCH HANDLER] ✅ Ответ отправлен, message_id={sent_message.message_id if sent_message else 'None'}")
                if user_id in user_search_state:
                    del user_search_state[user_id]
            except Exception as e:
                logger.error(f"[SEARCH HANDLER] ❌ Ошибка отправки результатов: {e}", exc_info=True)
                send_error_message(
                    message,
                    "❌ Ошибка при отправке результатов поиска. Попробуйте еще раз.",
                    state=state,
                    back_callback="back_to_start_menu"
                )
                if user_id in user_search_state:
                    del user_search_state[user_id]
                    
        except Exception as e:
            logger.error(f"[SEARCH HANDLER] Ошибка обработки: {e}", exc_info=True)
            send_error_message(
                message,
                "❌ Ошибка при выполнении поиска. Попробуйте еще раз.",
                state=state,
                back_callback="back_to_start_menu"
            )
    except Exception as e:
        logger.error(f"[SEARCH HANDLER] Критическая ошибка: {e}", exc_info=True)
        send_error_message(
            message,
            "❌ Не получилось обработать сообщение",
            back_callback="back_to_start_menu"
        )


# ==================== HANDLER ДЛЯ ИМПОРТА ====================

def check_import_message(message):
    """Проверяет, является ли сообщение ответом в состоянии импорта"""
    from moviebot.states import user_import_state
    user_id = message.from_user.id
    
    if user_id not in user_import_state:
        return False
    
    if not message.text or not message.text.strip():
        return False
    
    state = user_import_state[user_id]
    step = state.get('step')
    
    if step != 'waiting_user_id':
        return False
    
    # В личных чатах можно отвечать без реплая
    try:
        chat_info = bot_instance.get_chat(message.chat.id)
        is_private = chat_info.type == 'private'
    except:
        is_private = message.chat.id > 0
    
    is_reply = (message.reply_to_message and 
                message.reply_to_message.from_user and 
                message.reply_to_message.from_user.id == BOT_ID)
    
    if is_private:
        return True
    else:
        return is_reply


@bot_instance.message_handler(content_types=['text'], func=check_import_message)
def handle_import(message):
    """Обработчик для импорта из Кинопоиска"""
    logger.info(f"[IMPORT HANDLER] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    try:
        from moviebot.states import user_import_state
        user_id = message.from_user.id
        
        if user_id not in user_import_state:
            return
        
        state = user_import_state[user_id]
        step = state.get('step')
        
        if step == 'waiting_user_id':
            try:
                from moviebot.bot.handlers.series import handle_import_user_id_internal
                handle_import_user_id_internal(message, state)
            except Exception as e:
                logger.error(f"[IMPORT HANDLER] Ошибка обработки: {e}", exc_info=True)
                send_error_message(
                    message,
                    "❌ Не получилось обработать ID пользователя. Проверьте правильность ввода.",
                    state=state,
                    back_callback="settings:back"
                )
    except Exception as e:
        logger.error(f"[IMPORT HANDLER] Критическая ошибка: {e}", exc_info=True)
        send_error_message(
            message,
            "❌ Не получилось обработать сообщение",
            back_callback="settings:back"
        )


# ==================== HANDLER ДЛЯ РЕДАКТИРОВАНИЯ ====================

def check_edit_message(message):
    """Проверяет, является ли сообщение ответом в состоянии редактирования"""
    from moviebot.states import user_edit_state
    user_id = message.from_user.id
    
    if user_id not in user_edit_state:
        return False
    
    if not message.text or not message.text.strip():
        return False
    
    state = user_edit_state[user_id]
    action = state.get('action')
    
    if action not in ['edit_rating', 'edit_plan_datetime']:
        return False
    
    # Проверяем реплай
    is_reply = (message.reply_to_message and 
                message.reply_to_message.from_user and 
                message.reply_to_message.from_user.id == BOT_ID)
    
    prompt_message_id = state.get('prompt_message_id')
    
    # В личных чатах можно отвечать без реплая
    try:
        chat_info = bot_instance.get_chat(message.chat.id)
        is_private = chat_info.type == 'private'
    except:
        is_private = message.chat.id > 0
    
    if is_private:
        if prompt_message_id:
            if is_reply and message.reply_to_message.message_id == prompt_message_id:
                return True
            elif not is_reply:
                return True  # В личном чате можно без реплая
        else:
            return True
    else:
        # В группах требуется реплай
        if not is_reply:
            return False
        if prompt_message_id:
            return message.reply_to_message.message_id == prompt_message_id
        return True


@bot_instance.message_handler(content_types=['text'], func=check_edit_message)
def handle_edit(message):
    """Обработчик для редактирования"""
    logger.info(f"[EDIT HANDLER] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    try:
        from moviebot.states import user_edit_state
        user_id = message.from_user.id
        
        if user_id not in user_edit_state:
            return
        
        state = user_edit_state[user_id]
        action = state.get('action')
        
        try:
            if action == 'edit_rating':
                from moviebot.bot.handlers.rate import handle_edit_rating_internal
                handle_edit_rating_internal(message, state)
                return
            
            if action == 'edit_plan_datetime':
                is_reply = (message.reply_to_message and 
                           message.reply_to_message.from_user and 
                           message.reply_to_message.from_user.id == BOT_ID)
                
                prompt_message_id = state.get('prompt_message_id')
                
                # Проверяем, что это ответ на правильное сообщение
                try:
                    chat_info = bot_instance.get_chat(message.chat.id)
                    is_private = chat_info.type == 'private'
                except:
                    is_private = message.chat.id > 0
                
                if not is_private:
                    if not is_reply or (prompt_message_id and message.reply_to_message.message_id != prompt_message_id):
                        return
                elif prompt_message_id:
                    if is_reply and message.reply_to_message.message_id != prompt_message_id:
                        return
                
                from moviebot.bot.handlers.plan import handle_edit_plan_datetime_internal
                handle_edit_plan_datetime_internal(message, state)
                return
                
        except Exception as e:
            logger.error(f"[EDIT HANDLER] Ошибка обработки: {e}", exc_info=True)
            send_error_message(
                message,
                "❌ Не получилось обработать сообщение. Проверьте правильность ввода.",
                prompt_message_id=state.get('prompt_message_id'),
                state=state,
                back_callback="edit:back"
            )
    except Exception as e:
        logger.error(f"[EDIT HANDLER] Критическая ошибка: {e}", exc_info=True)
        send_error_message(
            message,
            "❌ Не получилось обработать сообщение",
            back_callback="edit:back"
        )


# ==================== HANDLER ДЛЯ НАСТРОЕК ====================

def check_settings_message(message):
    """Проверяет, является ли сообщение ответом в состоянии настроек"""
    from moviebot.states import user_settings_state
    user_id = message.from_user.id
    
    if user_id not in user_settings_state:
        return False
    
    if not message.text or not message.text.strip():
        return False
    
    state = user_settings_state.get(user_id)
    
    # Проверяем различные типы ожиданий в настройках
    if state.get('waiting_notify_time'):
        # Ожидаем время в формате ЧЧ:ММ
        time_str = message.text.strip()
        if ':' in time_str:
            return True
    
    if state.get('adding_reactions'):
        # Ожидаем эмодзи
        if message.reply_to_message:
            settings_msg_id = state.get('settings_msg_id')
            if settings_msg_id and message.reply_to_message.message_id == settings_msg_id:
                return True
    
    return False


@bot_instance.message_handler(content_types=['text'], func=check_settings_message)
def handle_settings(message):
    """Обработчик для настроек"""
    logger.info(f"[SETTINGS HANDLER] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    try:
        from moviebot.states import user_settings_state
        from moviebot.database.db_operations import set_notification_setting
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        if user_id not in user_settings_state:
            return
        
        state = user_settings_state.get(user_id)
        
        try:
            # Обработка времени напоминаний
            if state.get('waiting_notify_time'):
                time_str = message.text.strip()
                try:
                    if ':' in time_str:
                        parts = time_str.split(':')
                        if len(parts) == 2:
                            hour = int(parts[0])
                            minute = int(parts[1])
                            if 0 <= hour <= 23 and 0 <= minute <= 59:
                                notify_type = state.get('waiting_notify_time')
                                
                                if notify_type == 'home' or notify_type.startswith('home_'):
                                    if notify_type == 'home':
                                        set_notification_setting(chat_id, 'notify_home_weekday_hour', hour)
                                        set_notification_setting(chat_id, 'notify_home_weekday_minute', minute)
                                        bot_instance.reply_to(message, f"✅ Время напоминаний для домашнего просмотра установлено: {hour:02d}:{minute:02d}")
                                    elif notify_type == 'home_weekday':
                                        set_notification_setting(chat_id, 'notify_home_weekday_hour', hour)
                                        set_notification_setting(chat_id, 'notify_home_weekday_minute', minute)
                                        bot_instance.reply_to(message, f"✅ Время напоминаний для домашнего просмотра (будни) установлено: {hour:02d}:{minute:02d}")
                                    elif notify_type == 'home_weekend':
                                        set_notification_setting(chat_id, 'notify_home_weekend_hour', hour)
                                        set_notification_setting(chat_id, 'notify_home_weekend_minute', minute)
                                        bot_instance.reply_to(message, f"✅ Время напоминаний для домашнего просмотра (выходные) установлено: {hour:02d}:{minute:02d}")
                                
                                elif notify_type == 'cinema' or notify_type.startswith('cinema_'):
                                    if notify_type == 'cinema':
                                        set_notification_setting(chat_id, 'notify_cinema_weekday_hour', hour)
                                        set_notification_setting(chat_id, 'notify_cinema_weekday_minute', minute)
                                        bot_instance.reply_to(message, f"✅ Время напоминаний для просмотра в кино установлено: {hour:02d}:{minute:02d}")
                                    elif notify_type == 'cinema_weekday':
                                        set_notification_setting(chat_id, 'notify_cinema_weekday_hour', hour)
                                        set_notification_setting(chat_id, 'notify_cinema_weekday_minute', minute)
                                        bot_instance.reply_to(message, f"✅ Время напоминаний для просмотра в кино (будни) установлено: {hour:02d}:{minute:02d}")
                                    elif notify_type == 'cinema_weekend':
                                        set_notification_setting(chat_id, 'notify_cinema_weekend_hour', hour)
                                        set_notification_setting(chat_id, 'notify_cinema_weekend_minute', minute)
                                        bot_instance.reply_to(message, f"✅ Время напоминаний для просмотра в кино (выходные) установлено: {hour:02d}:{minute:02d}")
                                
                                if user_id in user_settings_state:
                                    del user_settings_state[user_id]
                                return
                            else:
                                send_error_message(
                                    message,
                                    "❌ Неверный формат времени. Используйте ЧЧ:ММ (например, 19:00 или 09:00)",
                                    state=state,
                                    back_callback="settings:back"
                                )
                                return
                except ValueError:
                    send_error_message(
                        message,
                        "❌ Неверный формат времени. Используйте ЧЧ:ММ (например, 19:00 или 09:00)",
                        state=state,
                        back_callback="settings:back"
                    )
                    return
                except Exception as e:
                    logger.error(f"[SETTINGS HANDLER] Ошибка сохранения времени: {e}", exc_info=True)
                    send_error_message(
                        message,
                        "❌ Произошла ошибка при сохранении времени.",
                        state=state,
                        back_callback="settings:back"
                    )
                    if user_id in user_settings_state:
                        del user_settings_state[user_id]
                    return
            
            # Обработка эмодзи
            if message.reply_to_message:
                settings_msg_id = state.get('settings_msg_id')
                if settings_msg_id and message.reply_to_message.message_id == settings_msg_id:
                    if state.get('adding_reactions'):
                        from moviebot.bot.handlers.settings import handle_settings_emojis
                        handle_settings_emojis(message)
                        return
                        
        except Exception as e:
            logger.error(f"[SETTINGS HANDLER] Ошибка обработки: {e}", exc_info=True)
            send_error_message(
                message,
                "❌ Не получилось обработать сообщение. Проверьте правильность ввода.",
                state=state,
                back_callback="settings:back"
            )
    except Exception as e:
        logger.error(f"[SETTINGS HANDLER] Критическая ошибка: {e}", exc_info=True)
        send_error_message(
            message,
            "❌ Не получилось обработать сообщение",
            back_callback="settings:back"
        )


# ==================== HANDLER ДЛЯ ОЧИСТКИ ====================

def check_clean_message(message):
    """Проверяет, является ли сообщение ответом в состоянии очистки"""
    from moviebot.states import user_clean_state
    user_id = message.from_user.id
    
    if user_id not in user_clean_state:
        return False
    
    if not message.text or not message.text.strip():
        return False
    
    # Проверяем реплай
    is_reply = (message.reply_to_message and 
                message.reply_to_message.from_user and 
                message.reply_to_message.from_user.id == BOT_ID)
    
    prompt_message_id = None
    state = user_clean_state[user_id]
    if 'prompt_message_id' in state:
        prompt_message_id = state.get('prompt_message_id')
    
    # В личных чатах можно отвечать без реплая
    try:
        chat_info = bot_instance.get_chat(message.chat.id)
        is_private = chat_info.type == 'private'
    except:
        is_private = message.chat.id > 0
    
    if is_private:
        if prompt_message_id:
            if is_reply and message.reply_to_message.message_id == prompt_message_id:
                return True
            elif not is_reply:
                return True  # В личном чате можно без реплая
        else:
            return True
    else:
        # В группах требуется реплай
        if not is_reply:
            return False
        if prompt_message_id:
            return message.reply_to_message.message_id == prompt_message_id
        return True


@bot_instance.message_handler(content_types=['text'], func=check_clean_message)
def handle_clean(message):
    """Обработчик для очистки"""
    logger.info(f"[CLEAN HANDLER] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    try:
        from moviebot.states import user_clean_state
        user_id = message.from_user.id
        text = message.text.strip().upper() if message.text else ""
        
        if user_id not in user_clean_state:
            return
        
        state = user_clean_state[user_id]
        
        try:
            if text == 'ДА, УДАЛИТЬ':
                from moviebot.bot.handlers.series import handle_clean_confirm_internal
                handle_clean_confirm_internal(message)
            else:
                send_error_message(
                    message,
                    "❌ Для подтверждения удаления введите: ДА, УДАЛИТЬ",
                    prompt_message_id=state.get('prompt_message_id'),
                    state=state,
                    back_callback="clean:back"
                )
        except Exception as e:
            logger.error(f"[CLEAN HANDLER] Ошибка обработки: {e}", exc_info=True)
            send_error_message(
                message,
                "❌ Не получилось обработать сообщение. Проверьте правильность ввода.",
                prompt_message_id=state.get('prompt_message_id'),
                state=state,
                back_callback="clean:back"
            )
    except Exception as e:
        logger.error(f"[CLEAN HANDLER] Критическая ошибка: {e}", exc_info=True)
        send_error_message(
            message,
            "❌ Не получилось обработать сообщение",
            back_callback="clean:back"
        )


# ==================== HANDLER ДЛЯ АДМИНСКИХ ФУНКЦИЙ ====================

def check_admin_message(message):
    """Проверяет, является ли сообщение ответом в админском состоянии"""
    from moviebot.states import (
        user_cancel_subscription_state, user_refund_state,
        user_unsubscribe_state, user_add_admin_state
    )
    user_id = message.from_user.id
    
    has_state = (
        user_id in user_cancel_subscription_state or
        user_id in user_refund_state or
        user_id in user_unsubscribe_state or
        user_id in user_add_admin_state
    )
    
    if not has_state:
        return False
    
    if not message.text or not message.text.strip():
        return False
    
    # В личных чатах можно отвечать без реплая (админские команды обычно в личных чатах)
    try:
        chat_info = bot_instance.get_chat(message.chat.id)
        is_private = chat_info.type == 'private'
    except:
        is_private = message.chat.id > 0
    
    # Для админских команд обычно требуется реплай, но в личных чатах можно без него
    if is_private:
        return True
    
    # В группах проверяем реплай
    is_reply = (message.reply_to_message and 
                message.reply_to_message.from_user and 
                message.reply_to_message.from_user.id == BOT_ID)
    return is_reply


@bot_instance.message_handler(content_types=['text'], func=check_admin_message)
def handle_admin(message):
    """Обработчик для админских функций"""
    logger.info(f"[ADMIN HANDLER] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    try:
        from moviebot.states import (
            user_cancel_subscription_state, user_refund_state,
            user_unsubscribe_state, user_add_admin_state
        )
        user_id = message.from_user.id
        chat_id = message.chat.id
        text = message.text.strip() if message.text else ""
        
        try:
            # Отмена подписки
            if user_id in user_cancel_subscription_state:
                state = user_cancel_subscription_state.get(user_id)
                if state:
                    state_chat_id = state.get('chat_id')
                    if state_chat_id and message.chat.id != state_chat_id:
                        return
                    
                    if text.upper().strip() == 'ДА, ОТМЕНИТЬ':
                        from moviebot.database.db_operations import cancel_subscription
                        subscription_id = state.get('subscription_id')
                        subscription_type = state.get('subscription_type')
                        
                        if subscription_id:
                            if cancel_subscription(subscription_id, user_id):
                                if subscription_type == 'group':
                                    bot_instance.reply_to(message, "✅ <b>Групповая подписка отменена</b>\n\nВаша групповая подписка была успешно отменена.", parse_mode='HTML')
                                else:
                                    bot_instance.reply_to(message, "✅ <b>Личная подписка отменена</b>\n\nВаша личная подписка была успешно отменена.", parse_mode='HTML')
                                del user_cancel_subscription_state[user_id]
                            else:
                                send_error_message(
                                    message,
                                    "❌ Ошибка отмены подписки. Попробуйте позже.",
                                    state=state,
                                    back_callback="payment:back"
                                )
                                del user_cancel_subscription_state[user_id]
                return
            
            # Возврат звезд
            if user_id in user_refund_state:
                state = user_refund_state.get(user_id)
                if state:
                    state_chat_id = state.get('chat_id')
                    if state_chat_id and message.chat.id != state_chat_id:
                        return
                    
                    charge_id = text.strip()
                    if charge_id:
                        del user_refund_state[user_id]
                        from moviebot.bot.handlers.stats import _process_refund
                        _process_refund(message, charge_id)
                return
            
            # Отмена подписки по ID
            if user_id in user_unsubscribe_state:
                state = user_unsubscribe_state[user_id]
                target_id_str = text.strip()
                if target_id_str:
                    try:
                        target_id = int(target_id_str)
                        is_group = target_id < 0
                        
                        from moviebot.bot.handlers.admin import cancel_subscription_by_id
                        success, result_message, count = cancel_subscription_by_id(target_id, is_group)
                        
                        if success:
                            text_result = f"✅ {result_message}\n\n"
                            text_result += f"ID: <code>{target_id}</code>\n"
                            text_result += f"Тип: {'Группа' if is_group else 'Пользователь'}"
                            
                            from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                            markup = InlineKeyboardMarkup()
                            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="admin:back"))
                            
                            bot_instance.reply_to(message, text_result, reply_markup=markup, parse_mode='HTML')
                        else:
                            send_error_message(
                                message,
                                f"❌ {result_message}",
                                state=state,
                                back_callback="admin:back"
                            )
                        
                        del user_unsubscribe_state[user_id]
                    except ValueError:
                        send_error_message(
                            message,
                            "❌ Неверный формат ID. Введите число.",
                            state=state,
                            back_callback="admin:back"
                        )
                return
            
            # Добавление администратора
            if user_id in user_add_admin_state:
                state = user_add_admin_state[user_id]
                admin_id_str = text.strip()
                if admin_id_str:
                    try:
                        admin_id = int(admin_id_str)
                        
                        from moviebot.utils.admin import add_admin
                        success, result_message = add_admin(admin_id, user_id)
                        
                        if success:
                            admin_text = "👑 <b>Вам выдан админский доступ</b>\n\n"
                            admin_text += "Доступные команды:\n\n"
                            admin_text += "<b>/unsubscribe</b> - Отменить подписку пользователя или группы\n"
                            admin_text += "   Введите ID пользователя или группы в ответном сообщении\n\n"
                            admin_text += "<b>/admin_stats</b> - Статистика бота\n"
                            admin_text += "   Показывает статистику пользователей, групп, подписок и т.д.\n\n"
                            admin_text += "<b>/refund_stars</b> - Возврат звезд\n"
                            admin_text += "   Введите charge_id платежа в ответном сообщении для возврата\n\n"
                            admin_text += "Все команды доступны только в личных сообщениях боту."
                            
                            try:
                                bot_instance.send_message(admin_id, admin_text, parse_mode='HTML')
                                logger.info(f"[ADMIN HANDLER] Уведомление отправлено новому администратору: {admin_id}")
                            except Exception as e:
                                logger.warning(f"[ADMIN HANDLER] Не удалось отправить уведомление администратору {admin_id}: {e}")
                            
                            text_result = f"✅ {result_message}\n\n"
                            text_result += f"ID администратора: <code>{admin_id}</code>\n\n"
                            text_result += "Уведомление отправлено новому администратору."
                            
                            from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                            markup = InlineKeyboardMarkup()
                            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="admin:back_to_list"))
                            
                            bot_instance.reply_to(message, text_result, reply_markup=markup, parse_mode='HTML')
                        else:
                            send_error_message(
                                message,
                                f"❌ {result_message}",
                                state=state,
                                back_callback="admin:back_to_list"
                            )
                        
                        del user_add_admin_state[user_id]
                    except ValueError:
                        send_error_message(
                            message,
                            "❌ Неверный формат ID. Введите число.",
                            state=state,
                            back_callback="admin:back_to_list"
                        )
                return
                
        except Exception as e:
            logger.error(f"[ADMIN HANDLER] Ошибка обработки: {e}", exc_info=True)
            send_error_message(
                message,
                "❌ Не получилось обработать сообщение. Проверьте правильность ввода.",
                back_callback="admin:back"
            )
    except Exception as e:
        logger.error(f"[ADMIN HANDLER] Критическая ошибка: {e}", exc_info=True)
        send_error_message(
            message,
            "❌ Не получилось обработать сообщение",
            back_callback="admin:back"
        )

