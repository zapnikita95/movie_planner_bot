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

