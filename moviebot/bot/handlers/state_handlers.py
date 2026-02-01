from moviebot.bot.bot_init import bot
"""
Отдельные handlers для каждого состояния пользователя
Каждый handler обрабатывает конкретный тип состояния с поддержкой:
- Реплаев на сообщения бота
- Личных чатов (можно отвечать без реплая)
- Обработки ошибок с кнопками
"""
import logging
import re
import sys
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.bot.bot_init import BOT_ID


logger = logging.getLogger(__name__)

# Логируем при импорте модуля


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
            'all': '💎 Movie Planner PRO'
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
        # Кнопку оплаты через Stars показываем только владельцу бота (для отладки)
        if user_id == 301810276:
            callback_data_stars = f"payment:pay_stars:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}:{payment_id_short}"
            markup.add(InlineKeyboardButton(f"⭐ Оплатить звездами Telegram ({stars_amount}⭐)", callback_data=callback_data_stars))
        callback_data_promo = f"payment:promo:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}:{payment_id_short}:{discounted_price}"
        markup.add(InlineKeyboardButton("🏷️ Промокод", callback_data=callback_data_promo))
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:subscribe:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}" if group_size else f"payment:subscribe:{sub_type}:{plan_type}:{period_type}"))
        
        try:
            bot.reply_to(message, text_result, reply_markup=markup, parse_mode='HTML')
        except Exception as e:
            logger.error(f"[PROMO HANDLER] Ошибка отправки: {e}", exc_info=True)
            bot.send_message(chat_id, text_result, reply_markup=markup, parse_mode='HTML')
        
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
        chat_info = bot.get_chat(chat_id)
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
        bot.reply_to(message, error_text, reply_markup=markup, parse_mode='HTML')
    except Exception as e:
        logger.error(f"[ERROR MESSAGE] Ошибка отправки сообщения об ошибке: {e}", exc_info=True)
        try:
            bot.send_message(message.chat.id, error_text, reply_markup=markup, parse_mode='HTML')
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
            prompt_message = bot.forward_message(chat_id, chat_id, prompt_message_id)
            # Или просто отправляем новое сообщение с тем же текстом
            # Для этого нужно сохранять текст промпта в состоянии
            bot.answer_callback_query(call.id, "Промпт отправлен повторно")
        except Exception as e:
            logger.error(f"[RETRY PROMPT] Ошибка: {e}", exc_info=True)
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
    except Exception as e:
        logger.error(f"[RETRY PROMPT] Ошибка обработки: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except:
            pass


def handle_cancel_action_callback(call):
    """Обработчик кнопки 'Отмена' - очищает состояние пользователя"""
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # Очищаем все состояния
        from moviebot.states import (
            user_ticket_state, user_search_state, user_import_state,
            user_edit_state, user_settings_state, user_plan_state,
            user_clean_state, user_promo_state, user_promo_admin_state,
            user_cancel_subscription_state, user_refund_state,
            user_unsubscribe_state, user_add_admin_state, user_view_film_state,
            user_private_handler_state
        )
        
        # Проверяем, в каком состоянии пользователь, чтобы вернуться в правильное место
        is_clean_state = user_id in user_clean_state
        
        states_to_clear = [
            user_ticket_state, user_search_state, user_import_state,
            user_edit_state, user_settings_state, user_plan_state,
            user_clean_state, user_promo_state, user_promo_admin_state,
            user_cancel_subscription_state, user_refund_state,
            user_unsubscribe_state, user_add_admin_state, user_view_film_state,
            user_private_handler_state
        ]
        
        for state_dict in states_to_clear:
            if user_id in state_dict:
                del state_dict[user_id]
        
        # Удаляем сообщение об ошибке
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        # Если это было состояние clean, возвращаемся в меню clean
        if is_clean_state:
            from moviebot.bot.handlers.settings.clean import clean_command
            class FakeMessage:
                def __init__(self, call):
                    self.from_user = call.from_user
                    self.chat = call.message.chat
                    self.text = '/clean'
                    self.message_id = call.message.message_id
            fake_msg = FakeMessage(call)
            clean_command(fake_msg)
        else:
            # Отправляем сообщение об отмене
            bot.send_message(chat_id, "❌ Действие отменено.")
    except Exception as e:
        logger.error(f"[CANCEL ACTION] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


# Регистрируем обработчики кнопок
@bot.callback_query_handler(func=lambda call: call.data.startswith("retry_prompt:"))
def retry_prompt_callback(call):
    handle_retry_prompt_callback(call)


@bot.callback_query_handler(func=lambda call: call.data == "cancel_action")
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
        chat_info = bot.get_chat(message.chat.id)
        if chat_info.type == 'private':
            return True
    except:
        if message.chat.id > 0:  # Положительные ID обычно личные чаты
            return True
    
    return False


@bot.message_handler(content_types=['text'], func=check_rating_message)
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
    
    text = message.text.strip()
    
    # Для обычного пользователя (оплата)
    if user_id in user_promo_state:
        # В личке можно без реплая, в группе — только реплай на промпт
        if message.reply_to_message:
            reply_text = message.reply_to_message.text or ""
            promo_prompts = ["Введите промокод", "Укажите промокод", "Промокод", "введите промокод"]
            if any(prompt.lower() in reply_text.lower() for prompt in promo_prompts):
                return True
        
        # В личных чатах — любой текст
        if message.chat.type == 'private':
            return True
        
        return False
    
    # Для админа (создание промокода) — только если формат похож на "КОД СКИДКА КОЛИЧЕСТВО"
    if user_id in user_promo_admin_state:
        parts = text.split()
        if len(parts) == 3:
            # Простая проверка, чтобы не ловить случайный текст
            try:
                int(parts[2])  # количество должно быть числом
                if parts[1].endswith('%') or parts[1].isdigit():
                    return True
            except:
                pass
        
        # Или если это реплай на меню /promo
        if message.reply_to_message and "Задайте промокод" in (message.reply_to_message.text or ""):
            return True
        
        if message.chat.type == 'private':
            return True
        
        return False
    
    return False


@bot.message_handler(content_types=['text'], func=check_promo_message)
def handle_promo(message):
    """Обработчик для промокодов"""
    logger.info(f"[PROMO HANDLER] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    
    from moviebot.states import user_promo_state, user_promo_admin_state
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()
    
    # Проверка на команду
    if message.text and message.text.startswith('/'):
        return  # Это команда → пропускаем
    
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
        
        # Обрабатываем промокод (вложенный try оставляем)
        promo_code = text.upper()
        logger.info(f"[PROMO HANDLER] Обработка промокода: {promo_code}")
        
        try:
            if not promo_code:
                send_error_message(
                    message,
                    "❌ Промокод не может быть пустым. Введите промокод.",
                    prompt_message_id=prompt_message_id,
                    state=state,
                    back_callback="payment:back_from_promo"
                )
                return
            
            # Проверка уже применённого промокода
            from moviebot.states import user_payment_state
            if user_id in user_payment_state:
                payment_state = user_payment_state[user_id]
                applied_promo = payment_state.get('promocode')
                applied_promo_id = payment_state.get('promocode_id')
                
                if applied_promo or applied_promo_id:
                    logger.warning(f"[PROMO HANDLER] Промокод уже применен")
                    error_text = "❌ Промокод уже применен к этому платежу.\n\nВы не можете применить промокод повторно."
                    markup = InlineKeyboardMarkup()
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:back_from_promo"))
                    bot.reply_to(message, error_text, reply_markup=markup, parse_mode='HTML')
                    return
            
            # Применяем промокод
            original_price = state.get('original_price', 0)
            if user_id in user_payment_state:
                original_price = user_payment_state[user_id].get('original_price', original_price)
            
            from moviebot.utils.promo import apply_promocode
            success, discounted_price, message_text, promocode_id = apply_promocode(
                promo_code, original_price, user_id, chat_id
            )
            
            if discounted_price < 0:
                discounted_price = 0
            
            if success:
                _process_promo_success(message, state, promo_code, discounted_price, message_text, promocode_id, user_id, chat_id)
            else:
                error_text = f"❌ {message_text}\n\nВведите другой промокод или оплатите полную стоимость."
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:back_from_promo"))
                bot.reply_to(message, error_text, reply_markup=markup)
                
        except Exception as e:
            logger.error(f"[PROMO HANDLER] Ошибка обработки промокода: {e}", exc_info=True)
            send_error_message(
                message,
                "❌ Не получилось обработать промокод.",
                prompt_message_id=prompt_message_id,
                state=state,
                back_callback="payment:back_from_promo"
            )
    
    elif user_id in user_promo_admin_state:
        logger.info(f"[PROMO ADMIN] Пользователь {user_id} в состоянии создания промокода")

        text = message.text.strip()
        if not text:
            bot.reply_to(message, "❌ Нельзя отправлять пустое сообщение")
            return

        # Проверяем, что это личка (для админ-команд не обязательно, но оставляем безопасность)
        if message.chat.type != 'private':
            if not message.reply_to_message:
                logger.info("[PROMO ADMIN] Пропущено — не реплай в группе")
                return

        parts = text.split(maxsplit=2)
        if len(parts) != 3:
            bot.reply_to(
                message,
                "❌ Неверный формат.\nПример: <code>DIM 95% 1</code> или <code>SALE500 20% 50</code>",
                parse_mode='HTML'
            )
            return

        code = parts[0].strip().upper()
        discount_input = parts[1].strip()
        try:
            total_uses = int(parts[2].strip())
            if total_uses < 1:
                raise ValueError("Количество должно быть ≥ 1")
        except ValueError as ve:
            bot.reply_to(
                message,
                f"❌ Ошибка в количестве использований: {ve}\nДолжно быть целое число ≥ 1"
            )
            return

        try:
            from moviebot.utils.promo import create_promocode
            success, result_message = create_promocode(code, discount_input, total_uses)

            if success:
                bot.reply_to(message, f"✅ Успешно создано!\n{result_message}", parse_mode='HTML')

                # Перезапускаем меню /promo
                from moviebot.bot.handlers.promo import promo_command
                promo_command(message)  # ← теперь напрямую, без фейкового сообщения

            else:
                bot.reply_to(message, f"❌ Не получилось создать:\n{result_message}", parse_mode='HTML')

            # Убираем состояние в любом случае
            user_promo_admin_state.pop(user_id, None)

        except Exception as e:
            logger.error(f"[PROMO ADMIN CREATE] Критическая ошибка: {e}", exc_info=True)
            bot.reply_to(message, "❌ Произошла ошибка при создании промокода. Попробуйте позже.")

# ==================== HANDLER ДЛЯ БИЛЕТОВ ====================

def check_ticket_text_reply(message):
    """Аналог check_plan_datetime_reply — точная проверка для текстовых шагов билетов"""
    from moviebot.states import user_ticket_state, is_user_in_valid_ticket_state
    
    is_private = message.chat.type == 'private'
    
    user_id = message.from_user.id
    if not is_user_in_valid_ticket_state(user_id):
        return False
    
    state = user_ticket_state[user_id]
    step = state.get('step')
    
    if step not in ['event_name', 'event_datetime']:
        return False
    
    if not message.text or not message.text.strip():
        return False
    
    # В группах — только reply на сообщение бота
    if not is_private:
        if not message.reply_to_message:
            return False
        if message.reply_to_message.from_user.id != BOT_ID:
            return False
        
        # Проверяем текст промпта
        reply_text = message.reply_to_message.text or ""
        if step == 'event_name' and "Напишите название мероприятия" not in reply_text:
            return False
        if step == 'event_datetime' and "Теперь укажите дату и время" not in reply_text:
            return False
        
        # Проверяем prompt_message_id, если сохранён
        prompt_message_id = state.get('prompt_message_id')
        if prompt_message_id and message.reply_to_message.message_id != prompt_message_id:
            return False
    
    else:
        # В личке — принимаем следующее сообщение или reply на правильный промпт
        if message.reply_to_message:
            # Если это реплай, проверяем что на правильный промпт
            if message.reply_to_message.from_user.id != BOT_ID:
                return False
            reply_text = message.reply_to_message.text or ""
            if step == 'event_name' and "Напишите название мероприятия" not in reply_text:
                return False
            if step == 'event_datetime' and "Теперь укажите дату и время" not in reply_text:
                return False
            
            prompt_message_id = state.get('prompt_message_id')
            if prompt_message_id and message.reply_to_message.message_id != prompt_message_id:
                return False
        # Если не reply — принимаем следующее сообщение (в личке можно без реплая)
    
    return True


@bot.message_handler(content_types=['text'], func=check_ticket_text_reply)
def handle_ticket_text_reply(message):
    """Обработчик текстовых шагов для добавления мероприятия (название + дата/время)"""
    user_id = message.from_user.id
    text = message.text.strip()
    chat_id = message.chat.id
    
    logger.info(f"[TICKET TEXT REPLY] user_id={user_id}, text='{text}'")
    
    try:
        from moviebot.states import user_ticket_state
        state = user_ticket_state[user_id]
        step = state['step']
        ticket_type = state.get('type')
        
        if ticket_type != 'event':
            return
        
        # ==================== НАЗВАНИЕ МЕРОПРИЯТИЯ ====================
        if step == 'event_name':
            if not text:
                bot.reply_to(message, "❌ Название не может быть пустым.")
                return
            
            state.update({
                'step': 'event_datetime',
                'event_name': text
            })
            
            # Отправляем промпт и сохраняем message_id
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
            
            sent = bot.reply_to(
                message,
                f"🎤 <b>{text}</b>\n\n"
                f"Теперь укажите <b>дату и время</b>:\n\n"
                f"Примеры:\n"
                f"• 15 января 20:30\n"
                f"• 15.01 20:30\n"
                f"• завтра 19:00\n"
                f"• 20:00 (если сегодня)",
                parse_mode='HTML',
                reply_markup=markup
            )
            state['prompt_message_id'] = sent.message_id
            return
        
        # ==================== ДАТА/ВРЕМЯ МЕРОПРИЯТИЯ ====================
        if step == 'event_datetime':
            # Используем тот же механизм, что и для планирования фильмов (get_plan_day_or_date_internal)
            from moviebot.utils.parsing import parse_session_time
            from moviebot.database.db_operations import get_user_timezone_or_default
            import pytz
            from moviebot.database.db_connection import db_lock, cursor, connection
            from datetime import datetime, timedelta
            import re
            
            user_tz = get_user_timezone_or_default(user_id)
            now = datetime.now(user_tz)
            
            # Используем parse_session_time для более полной обработки дат (как в get_plan_day_or_date_internal)
            plan_dt = parse_session_time(text, user_tz)
            
            if not plan_dt:
                # Если parse_session_time не сработал, пробуем parse_relative_or_absolute_time
                from moviebot.utils.parsing import parse_relative_or_absolute_time
                plan_dt = parse_relative_or_absolute_time(text, user_id)
            
            # Если всё ещё не распознано, пробуем логику из get_plan_day_or_date_internal
            if not plan_dt:
                text_lower = text.lower().strip()
                extracted_time = None
                
                # Ищем время в формате ЧЧ:ММ
                time_match = re.search(r'\b(\d{1,2}):(\d{2})\b', text)
                if time_match:
                    hour = int(time_match.group(1))
                    minute = int(time_match.group(2))
                    if 0 <= hour <= 23 and 0 <= minute <= 59:
                        extracted_time = (hour, minute)
                
                # Пробуем распознать "сегодня", "завтра"
                if 'сегодня' in text_lower:
                    plan_date = now.date()
                    if extracted_time:
                        hour, minute = extracted_time
                    else:
                        hour, minute = 20, 0  # По умолчанию 20:00 для мероприятий
                    plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour, minute=minute))
                    plan_dt = user_tz.localize(plan_dt)
                elif 'завтра' in text_lower:
                    plan_date = (now.date() + timedelta(days=1))
                    if extracted_time:
                        hour, minute = extracted_time
                    else:
                        hour, minute = 20, 0  # По умолчанию 20:00 для мероприятий
                    plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour, minute=minute))
                    plan_dt = user_tz.localize(plan_dt)
                elif extracted_time:
                    # Если есть только время без даты, используем сегодня
                    hour, minute = extracted_time
                    plan_date = now.date()
                    plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour, minute=minute))
                    plan_dt = user_tz.localize(plan_dt)
                    # Если время уже прошло, используем завтра
                    if plan_dt < now:
                        plan_date = (now.date() + timedelta(days=1))
                        plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour, minute=minute))
                        plan_dt = user_tz.localize(plan_dt)
            
            if not plan_dt:
                sent = bot.reply_to(
                    message,
                    "❌ Не понял дату и время 😔\n\n"
                    "Попробуйте ещё раз:\n"
                    "• 15 января 20:30\n"
                    "• завтра 19:00\n"
                    "• 20:00",
                    parse_mode='HTML'
                )
                state['prompt_message_id'] = sent.message_id
                return
            
            # Создаём план для мероприятия (film_id = NULL, custom_title = название мероприятия)
            with db_lock:
                cursor.execute('''
                    INSERT INTO plans (chat_id, user_id, plan_datetime, plan_type, custom_title, film_id)
                    VALUES (%s, %s, %s, 'cinema', %s, NULL)
                    RETURNING id
                ''', (chat_id, user_id, plan_dt.astimezone(pytz.utc), state['event_name']))
                plan_id = cursor.fetchone()[0]
                connection.commit()
            
            # Переход к загрузке билетов (TTL 15 мин)
            import time
            user_ticket_state[user_id] = {
                'step': 'upload_ticket',
                'plan_id': plan_id,
                'chat_id': chat_id,
                'type': 'event',
                'created_at': time.time()
            }
            
            dt_local = plan_dt.astimezone(user_tz)
            date_str = dt_local.strftime('%d.%m.%Y в %H:%M')
            
            bot.reply_to(
                message,
                f"🎤 <b>{state['event_name']}</b>\n"
                f"📅 <b>{date_str}</b>\n\n"
                f"Супер! Теперь отправьте <b>фото/файлы билетов</b>.\n"
                f"Можно несколько сообщений.\n"
                f"Когда всё — напишите <code>готово</code>.",
                parse_mode='HTML'
            )
            return
            
    except Exception as e:
        logger.error(f"[TICKET TEXT REPLY] Ошибка: {e}", exc_info=True)
        bot.reply_to(message, "❌ Произошла ошибка. Начните заново.")
        if user_id in user_ticket_state:
            del user_ticket_state[user_id]


# Сохраняем твой существующий check_ticket_message только для "готово" в upload/add_more
def check_ticket_done(message):
    from moviebot.states import user_ticket_state, is_user_in_valid_ticket_state
    user_id = message.from_user.id
    if not is_user_in_valid_ticket_state(user_id):
        return False
    step = user_ticket_state[user_id].get('step')
    return step in ['upload_ticket', 'add_more_tickets'] and message.text.lower().strip() == 'готово'

@bot.message_handler(content_types=['text'], func=check_ticket_done)
def handle_ticket_done(message):
    # твоя существующая логика для "готово"
    # (оставь как было)
    pass

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
        chat_info = bot.get_chat(message.chat.id)
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


@bot.message_handler(content_types=['text'], func=check_search_message)
def handle_search(message):
    """Обработчик для поиска"""
    logger.info(f"[SEARCH HANDLER] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    try:
        from moviebot.states import user_search_state
        from moviebot.bot.handlers.series import search_films_with_type
        
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
            # /search не должен быть частью запроса — убираем в начале
            query = re.sub(r'^/search(@\w+)?\s*', '', query, flags=re.IGNORECASE).strip()
            if not query:
                return
            search_type = state.get('search_type', 'mixed')
            
            logger.info(f"[SEARCH HANDLER] Поиск по запросу '{query}' от пользователя {user_id}, тип: {search_type}")
            
            if search_type == 'people':
                from moviebot.api.kinopoisk_api import search_persons
                persons, _ = search_persons(query, page=1)
                if not persons:
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("🔄 Повторить поиск", callback_data="search:retry"))
                    markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                    bot.reply_to(message, f"❌ По запросу «{query}» людей не найдено.", reply_markup=markup)
                    if user_id in user_search_state:
                        del user_search_state[user_id]
                    return
                results_text = "👥 Вот люди из киносферы, найденные по вашему запросу:\n\n"
                markup = InlineKeyboardMarkup(row_width=1)
                for p in persons[:20]:
                    pid = p.get('kinopoiskId')
                    name = p.get('nameRu') or p.get('nameEn') or 'Без имени'
                    if pid:
                        btn = (name[:60] + "…") if len(name) > 60 else name
                        markup.add(InlineKeyboardButton(btn, callback_data=f"person_select:{pid}"))
                markup.add(InlineKeyboardButton("🔄 Повторить поиск", callback_data="search:retry"))
                markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                sent = bot.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
                user_search_state[user_id] = {
                    'chat_id': chat_id, 'message_id': sent.message_id if sent else None,
                    'search_type': 'people', 'people_query': query, 'people_results': persons[:20],
                }
                logger.info(f"[SEARCH HANDLER] Люди: отправлено {len(persons)} результатов")
                return
            
            films, total_pages = search_films_with_type(query, page=1, search_type=search_type)
            logger.info(f"[SEARCH HANDLER] ✅ Поиск завершен: найдено {len(films) if films else 0} результатов, страниц: {total_pages}")
            
            if not films:
                bot.reply_to(message, f"❌ Ничего не найдено по запросу '{query}'")
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
                    _r = film.get('ratingKinopoisk') or film.get('rating') or film.get('ratingImdb')
                    rating = None
                    if _r is not None and str(_r).strip().lower() not in ('', 'null', 'none', 'n/a'):
                        rating = _r
                    kp_id = film.get('kinopoiskId') or film.get('filmId') or film.get('id')
                    
                    film_type = film.get('type', '').upper() if film.get('type') else 'FILM'
                    is_series = film_type in ('TV_SERIES', 'MINI_SERIES')
                    
                    if kp_id:
                        type_indicator = "📺" if is_series else "🎬"
                        button_text = f"{type_indicator} {title} ({year})"
                        if len(button_text) > 50:
                            button_text = button_text[:47] + "..."
                        results_text += f"• {type_indicator} <b>{title}</b> ({year})"
                        if rating:
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
                sent_message = bot.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
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
        chat_info = bot.get_chat(message.chat.id)
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


@bot.message_handler(content_types=['text'], func=check_import_message)
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
        chat_info = bot.get_chat(message.chat.id)
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


@bot.message_handler(content_types=['text'], func=check_edit_message)
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
                    chat_info = bot.get_chat(message.chat.id)
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
    
    state = user_settings_state.get(user_id)
    
    # Проверяем различные типы ожиданий в настройках
    if state.get('waiting_notify_time'):
        # Ожидаем время в формате ЧЧ:ММ
        if not message.text or not message.text.strip():
            return False
        
        # Проверяем формат времени
        time_str = message.text.strip()
        if ':' not in time_str:
            return False
        
        # Проверяем реплай для групповых чатов
        try:
            chat_info = bot.get_chat(message.chat.id)
            is_private = chat_info.type == 'private'
        except:
            is_private = message.chat.id > 0
        
        if not is_private:
            # В группах требуется реплай
            if not message.reply_to_message:
                return False
            # Проверяем, что реплай на сообщение бота
            if not message.reply_to_message.from_user:
                return False
            if message.reply_to_message.from_user.id != BOT_ID:
                return False
            # Проверяем, что реплай на правильное сообщение (если есть prompt_message_id)
            prompt_message_id = state.get('prompt_message_id')
            if prompt_message_id and message.reply_to_message.message_id != prompt_message_id:
                return False
        
        # Проверяем формат времени ЧЧ:ММ
        try:
            parts = time_str.split(':')
            if len(parts) == 2:
                hour = int(parts[0])
                minute = int(parts[1])
                if 0 <= hour <= 23 and 0 <= minute <= 59:
                    return True
        except:
            pass
        
        return False
    
    if state.get('adding_reactions'):
        # Ожидаем эмодзи - может быть только эмодзи, без текста
        if message.reply_to_message:
            settings_msg_id = state.get('settings_msg_id')
            if settings_msg_id and message.reply_to_message.message_id == settings_msg_id:
                # Проверяем, что есть текст (даже если только эмодзи)
                if message.text and message.text.strip():
                    return True
    
    return False


@bot.message_handler(content_types=['text'], func=check_settings_message)
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
                                        bot.reply_to(message, f"✅ Время напоминаний для домашнего просмотра установлено: {hour:02d}:{minute:02d}")
                                    elif notify_type == 'home_weekday':
                                        set_notification_setting(chat_id, 'notify_home_weekday_hour', hour)
                                        set_notification_setting(chat_id, 'notify_home_weekday_minute', minute)
                                        bot.reply_to(message, f"✅ Время напоминаний для домашнего просмотра (будни) установлено: {hour:02d}:{minute:02d}")
                                    elif notify_type == 'home_weekend':
                                        set_notification_setting(chat_id, 'notify_home_weekend_hour', hour)
                                        set_notification_setting(chat_id, 'notify_home_weekend_minute', minute)
                                        bot.reply_to(message, f"✅ Время напоминаний для домашнего просмотра (выходные) установлено: {hour:02d}:{minute:02d}")
                                
                                elif notify_type == 'cinema' or notify_type.startswith('cinema_'):
                                    if notify_type == 'cinema':
                                        set_notification_setting(chat_id, 'notify_cinema_weekday_hour', hour)
                                        set_notification_setting(chat_id, 'notify_cinema_weekday_minute', minute)
                                        bot.reply_to(message, f"✅ Время напоминаний для просмотра в кино установлено: {hour:02d}:{minute:02d}")
                                    elif notify_type == 'cinema_weekday':
                                        set_notification_setting(chat_id, 'notify_cinema_weekday_hour', hour)
                                        set_notification_setting(chat_id, 'notify_cinema_weekday_minute', minute)
                                        bot.reply_to(message, f"✅ Время напоминаний для просмотра в кино (будни) установлено: {hour:02d}:{minute:02d}")
                                    elif notify_type == 'cinema_weekend':
                                        set_notification_setting(chat_id, 'notify_cinema_weekend_hour', hour)
                                        set_notification_setting(chat_id, 'notify_cinema_weekend_minute', minute)
                                        bot.reply_to(message, f"✅ Время напоминаний для просмотра в кино (выходные) установлено: {hour:02d}:{minute:02d}")
                                
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
                        from moviebot.bot.handlers.settings_main import handle_settings_emojis
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
        chat_info = bot.get_chat(message.chat.id)
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


@bot.message_handler(content_types=['text'], func=check_clean_message)
def handle_clean(message):
    """Обработчик для очистки"""
    logger.info(f"[CLEAN HANDLER] ===== START: message_id={message.message_id}, user_id={message.from_user.id}, text='{message.text}'")
    try:
        from moviebot.states import user_clean_state
        user_id = message.from_user.id
        text = message.text.strip() if message.text else ""
        text_upper = text.upper()
        
        if user_id not in user_clean_state:
            logger.warning(f"[CLEAN HANDLER] Пользователь {user_id} не в состоянии user_clean_state")
            return
        
        state = user_clean_state[user_id]
        target = state.get('target')
        logger.info(f"[CLEAN HANDLER] Пользователь {user_id} в состоянии, target={target}")
        
        try:
            # Нормализуем текст: убираем пробелы, запятые, приводим к верхнему регистру
            normalized_text = text_upper.replace(' ', '').replace(',', '').replace('.', '')
            logger.info(f"[CLEAN HANDLER] Нормализованный текст: '{normalized_text}'")
            
            # Проверяем различные варианты написания "ДА, УДАЛИТЬ"
            if normalized_text == 'ДАУДАЛИТЬ':
                logger.info(f"[CLEAN HANDLER] Подтверждение получено, вызываю handle_clean_confirm_internal для target={target}")
                from moviebot.bot.handlers.series import handle_clean_confirm_internal
                handle_clean_confirm_internal(message)
            else:
                logger.warning(f"[CLEAN HANDLER] Текст не соответствует 'ДА, УДАЛИТЬ': '{text}' (нормализовано: '{normalized_text}')")
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
    # КРИТИЧНО: Проверяем команды САМЫМ ПЕРВЫМ делом, ДО всех остальных проверок
    # Это гарантирует, что команды никогда не будут обработаны админским хендлером
    if message.text and message.text.strip().startswith('/'):
        logger.info(f"[CHECK ADMIN MESSAGE] ❌ Это команда, НЕ обрабатываем админским хендлером: text='{message.text[:50]}'")
        return False
    
    from moviebot.states import (
        user_cancel_subscription_state, user_refund_state,
        user_unsubscribe_state, user_add_admin_state, user_promo_admin_state
    )
    user_id = message.from_user.id
    text = message.text.strip() if message.text else ""
    
    # Определяем тип чата
    try:
        chat_info = bot.get_chat(message.chat.id)
        is_private = chat_info.type == 'private'
    except:
        is_private = message.chat.id > 0
    
    # Проверяем наличие состояния
    has_unsubscribe = user_id in user_unsubscribe_state
    has_add_admin = user_id in user_add_admin_state
    has_promo_admin = user_id in user_promo_admin_state
    has_refund = user_id in user_refund_state
    has_cancel_sub = user_id in user_cancel_subscription_state
    
    logger.info(f"[CHECK ADMIN MESSAGE] user_id={user_id}, text='{text[:50]}', is_private={is_private}, "
                f"has_unsubscribe={has_unsubscribe}, has_add_admin={has_add_admin}, "
                f"has_promo_admin={has_promo_admin}, has_refund={has_refund}, has_cancel_sub={has_cancel_sub}")
    
    if not (has_unsubscribe or has_add_admin or has_promo_admin or has_refund or has_cancel_sub):
        logger.debug(f"[CHECK ADMIN MESSAGE] Нет админских состояний для user_id={user_id}")
        return False
    
    if not message.text or not text:
        logger.debug(f"[CHECK ADMIN MESSAGE] Нет текста: text='{text}'")
        return False
    
    # В личных чатах принимаем следующее сообщение (как в is_expected_text_in_private)
    # Без проверки формата - обработаем в handle_admin, там покажем ошибку если формат неверный
    if is_private:
        logger.info(f"[CHECK ADMIN MESSAGE] ✅ Принимаем сообщение в личке для user_id={user_id} (любой текст)")
        return True
    
    # В группах требуется реплай на бота
    is_reply = (message.reply_to_message and 
                message.reply_to_message.from_user and 
                message.reply_to_message.from_user.id == BOT_ID)
    
    if not is_reply:
        logger.debug(f"[CHECK ADMIN MESSAGE] В группе требуется реплай, но его нет")
        return False
    
    # В группах проверяем, что это реплай на правильное сообщение (если есть prompt_message_id)
    # Но для промокодов, unsubscribe и add_admin в личке можно без реплая - уже обработано выше
    logger.info(f"[CHECK ADMIN MESSAGE] ✅ Принимаем сообщение в группе для user_id={user_id}")
    return True


# Обработчик должен быть зарегистрирован ДО main_text_handler
# Используем более высокий приоритет через content_types
# ВАЖНО: check_admin_message возвращает False для команд, поэтому команды не будут перехвачены
@bot.message_handler(content_types=['text'], func=check_admin_message)
def handle_admin(message):
    """Обработчик для админских функций (промокоды, админы, unsubscribe)"""
    logger.info(f"[ADMIN HANDLER] ===== START: message_id={message.message_id}, user_id={message.from_user.id}, text='{message.text[:50] if message.text else ''}'")
    try:
        from moviebot.states import (
            user_cancel_subscription_state, user_refund_state,
            user_unsubscribe_state, user_add_admin_state, user_promo_admin_state
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
                                    bot.reply_to(message, "✅ <b>Групповая подписка отменена</b>\n\nВаша групповая подписка была успешно отменена.", parse_mode='HTML')
                                else:
                                    bot.reply_to(message, "✅ <b>Личная подписка отменена</b>\n\nВаша личная подписка была успешно отменена.", parse_mode='HTML')
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
                    
                    # Проверяем, что сообщение является реплаем на prompt_message_id
                    prompt_message_id = state.get('prompt_message_id')
                    if prompt_message_id:
                        if not message.reply_to_message or message.reply_to_message.message_id != prompt_message_id:
                            logger.info(f"[REFUND] Сообщение не является реплаем на prompt_message_id={prompt_message_id}, игнорируем")
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
                logger.info(f"[UNSUBSCRIBE] Обработка: text='{text}', state={state}")
                
                # В личке принимаем следующее сообщение (любой текст)
                # В группах требуется реплай на бота
                try:
                    chat_info = bot.get_chat(message.chat.id)
                    is_private = chat_info.type == 'private'
                except:
                    is_private = message.chat.id > 0
                
                if not is_private:
                    # В группах требуется реплай на бота
                    is_reply = (message.reply_to_message and 
                                message.reply_to_message.from_user and 
                                message.reply_to_message.from_user.id == BOT_ID)
                    if not is_reply:
                        logger.info(f"[UNSUBSCRIBE] В группе требуется реплай, но его нет, игнорируем")
                        return
                
                # Парсим chat_id: число или отрицательное число
                target_id_str = text.strip()
                logger.info(f"[UNSUBSCRIBE] Получен target_id_str: '{target_id_str}'")
                
                if target_id_str:
                    try:
                        # Unsubscribe может быть отрицательным числом (для групп) или положительным chat_id группы
                        target_id = int(target_id_str)
                        
                        # Проверяем, является ли это группой
                        # 1. Если отрицательное число - это точно группа
                        # 2. Если положительное - проверяем в БД, есть ли подписка с таким chat_id и subscription_type='group'
                        is_group = target_id < 0
                        
                        if not is_group and target_id > 0:
                            # Проверяем в БД, есть ли групповая подписка с таким chat_id
                            from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
                            conn_check = get_db_connection()
                            cursor_check = get_db_cursor()
                            try:
                                with db_lock:
                                    cursor_check.execute("""
                                        SELECT id FROM subscriptions 
                                        WHERE chat_id = %s AND subscription_type = 'group' AND is_active = TRUE
                                        LIMIT 1
                                    """, (target_id,))
                                    group_sub = cursor_check.fetchone()
                                    if group_sub:
                                        is_group = True
                                        logger.info(f"[UNSUBSCRIBE] Найдена групповая подписка для chat_id={target_id}, определяем как группу")
                            finally:
                                try:
                                    cursor_check.close()
                                except:
                                    pass
                                try:
                                    conn_check.close()
                                except:
                                    pass
                        
                        logger.info(f"[UNSUBSCRIBE] Парсинг: target_id={target_id}, is_group={is_group}")
                        
                        # Если это группа, отменяем сразу (как раньше)
                        if is_group:
                            from moviebot.bot.handlers.admin import cancel_subscription_by_id
                            logger.info(f"[UNSUBSCRIBE] Отмена подписки для группы: target_id={target_id}")
                            success, result_message, count = cancel_subscription_by_id(target_id, is_group)
                            
                            logger.info(f"[UNSUBSCRIBE] Результат отмены: success={success}, message='{result_message}', count={count}")
                            
                            try:
                                chat_info = bot.get_chat(message.chat.id)
                                is_private = chat_info.type == 'private'
                            except:
                                is_private = message.chat.id > 0
                            
                            if success:
                                text_result = f"✅ {result_message}\n\n"
                                text_result += f"ID: <code>{target_id}</code>\n"
                                text_result += f"Тип: Группа"
                                
                                markup = InlineKeyboardMarkup()
                                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="admin:back"))
                                
                                if is_private:
                                    bot.send_message(message.chat.id, text_result, reply_markup=markup, parse_mode='HTML')
                                else:
                                    bot.reply_to(message, text_result, reply_markup=markup, parse_mode='HTML')
                                logger.info(f"[UNSUBSCRIBE] ✅ Сообщение об успехе отправлено")
                            else:
                                error_text = f"❌ {result_message}"
                                if is_private:
                                    bot.send_message(message.chat.id, error_text)
                                else:
                                    send_error_message(
                                        message,
                                        error_text,
                                        state=state,
                                        back_callback="admin:back"
                                    )
                                logger.warning(f"[UNSUBSCRIBE] ❌ Ошибка: {result_message}")
                            
                            if user_id in user_unsubscribe_state:
                                del user_unsubscribe_state[user_id]
                                logger.info(f"[UNSUBSCRIBE] Состояние очищено")
                        else:
                            # Если это пользователь, показываем меню выбора типа отмены
                            
                            text_result = f"👤 <b>Пользователь: {target_id}</b>\n\n"
                            text_result += "Что вы хотите отменить?\n\n"
                            text_result += "• <b>Личная подписка</b> - все личные подписки этого пользователя\n"
                            text_result += "• <b>Оплаченные подписки</b> - все подписки, которые были оплачены этим пользователем (личные и групповые)"
                            
                            markup = InlineKeyboardMarkup(row_width=1)
                            markup.add(InlineKeyboardButton("👤 Личная подписка", callback_data=f"unsubscribe:personal:{target_id}"))
                            markup.add(InlineKeyboardButton("💳 Оплаченные подписки", callback_data=f"unsubscribe:paid:{target_id}"))
                            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="admin:back"))
                            
                            # Сохраняем target_id в состоянии для дальнейшей обработки
                            state['target_id'] = target_id
                            state['prompt_message_id'] = None  # Сбрасываем, так как теперь будем работать через callbacks
                            
                            try:
                                chat_info = bot.get_chat(message.chat.id)
                                is_private = chat_info.type == 'private'
                            except:
                                is_private = message.chat.id > 0
                            
                            if is_private:
                                bot.send_message(message.chat.id, text_result, reply_markup=markup, parse_mode='HTML')
                            else:
                                bot.reply_to(message, text_result, reply_markup=markup, parse_mode='HTML')
                            
                            logger.info(f"[UNSUBSCRIBE] ✅ Меню выбора типа отмены отправлено для target_id={target_id}")
                            # НЕ удаляем user_unsubscribe_state, так как будем обрабатывать через callbacks
                    except ValueError:
                        error_text = "❌ Неверный формат ID. Введите число (положительное для пользователя, отрицательное для группы)."
                        logger.warning(f"[UNSUBSCRIBE] Неверный формат ID: '{target_id_str}'")
                        try:
                            chat_info = bot.get_chat(message.chat.id)
                            is_private = chat_info.type == 'private'
                        except:
                            is_private = message.chat.id > 0
                        
                        if is_private:
                            bot.send_message(message.chat.id, error_text)
                        else:
                            send_error_message(
                                message,
                                error_text,
                                state=state,
                                back_callback="admin:back"
                            )
                return
            
            # Добавление администратора
            if user_id in user_add_admin_state:
                state = user_add_admin_state[user_id]
                logger.info(f"[ADD_ADMIN] Обработка: text='{text}', state={state}")
                
                # В личке можно отвечать следующим сообщением (любой текст)
                # В группах требуется реплай на бота
                try:
                    chat_info = bot.get_chat(message.chat.id)
                    is_private = chat_info.type == 'private'
                except:
                    is_private = message.chat.id > 0
                
                if not is_private:
                    # В группах требуется реплай на бота
                    is_reply = (message.reply_to_message and 
                                message.reply_to_message.from_user and 
                                message.reply_to_message.from_user.id == BOT_ID)
                    if not is_reply:
                        logger.info(f"[ADD_ADMIN] В группе требуется реплай, но его нет, игнорируем")
                        return
                
                # Парсим user_id: число
                admin_id_str = text.strip()
                if admin_id_str:
                    try:
                        admin_id = int(admin_id_str)
                        
                        from moviebot.utils.admin import add_admin
                        logger.info(f"[ADD_ADMIN] Вызываю add_admin(admin_id={admin_id}, added_by={user_id})")
                        success, result_message = add_admin(admin_id, user_id)
                        
                        logger.info(f"[ADD_ADMIN] Результат: success={success}, message='{result_message}'")
                        
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
                                bot.send_message(admin_id, admin_text, parse_mode='HTML')
                                logger.info(f"[ADD_ADMIN] ✅ Уведомление отправлено новому администратору: {admin_id}")
                            except Exception as e:
                                logger.warning(f"[ADD_ADMIN] ⚠️ Не удалось отправить уведомление администратору {admin_id}: {e}")
                            
                            text_result = f"✅ {result_message}\n\n"
                            text_result += f"ID администратора: <code>{admin_id}</code>\n\n"
                            text_result += "Уведомление отправлено новому администратору."
                            
                            markup = InlineKeyboardMarkup()
                            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="admin:back_to_list"))
                            
                            try:
                                chat_info = bot.get_chat(message.chat.id)
                                is_private = chat_info.type == 'private'
                            except:
                                is_private = message.chat.id > 0
                            
                            if is_private:
                                bot.send_message(message.chat.id, text_result, reply_markup=markup, parse_mode='HTML')
                            else:
                                bot.reply_to(message, text_result, reply_markup=markup, parse_mode='HTML')
                            
                            logger.info(f"[ADD_ADMIN] ✅ Сообщение об успехе отправлено")
                        else:
                            error_text = f"❌ {result_message}"
                            try:
                                chat_info = bot.get_chat(message.chat.id)
                                is_private = chat_info.type == 'private'
                            except:
                                is_private = message.chat.id > 0
                            
                            if is_private:
                                bot.send_message(message.chat.id, error_text)
                            else:
                                send_error_message(
                                    message,
                                    error_text,
                                    state=state,
                                    back_callback="admin:back_to_list"
                                )
                            logger.warning(f"[ADD_ADMIN] ❌ Ошибка: {result_message}")
                        
                        # Удаляем состояние после обработки (успех или ошибка)
                        if user_id in user_add_admin_state:
                            del user_add_admin_state[user_id]
                            logger.info(f"[ADD_ADMIN] Состояние очищено")
                    except ValueError:
                        error_text = "❌ Неверный формат ID. Введите число."
                        logger.warning(f"[ADD_ADMIN] Неверный формат ID: '{admin_id_str}'")
                        try:
                            chat_info = bot.get_chat(message.chat.id)
                            is_private = chat_info.type == 'private'
                        except:
                            is_private = message.chat.id > 0
                        
                        if is_private:
                            bot.send_message(message.chat.id, error_text)
                        else:
                            send_error_message(
                                message,
                                error_text,
                                state=state,
                                back_callback="admin:back_to_list"
                            )
                return
            
            # Обработка промокодов (/promo)
            if user_id in user_promo_admin_state:
                state = user_promo_admin_state[user_id]
                logger.info(f"[PROMO ADMIN] Обработка: text='{text}', state={state}")
                
                # В личке можно отвечать следующим сообщением (любой текст)
                # В группах требуется реплай на бота
                try:
                    chat_info = bot.get_chat(message.chat.id)
                    is_private = chat_info.type == 'private'
                except:
                    is_private = message.chat.id > 0
                
                if not is_private:
                    # В группах требуется реплай на бота
                    is_reply = (message.reply_to_message and 
                                message.reply_to_message.from_user and 
                                message.reply_to_message.from_user.id == BOT_ID)
                    if not is_reply:
                        logger.info(f"[PROMO ADMIN] В группе требуется реплай, но его нет, игнорируем")
                        return
                
                # Парсим промокод: КОД СКИДКА КОЛИЧЕСТВО
                # Формат: *символы любые* пробел *число или процент* пробел *число*
                parts = text.strip().split()
                if len(parts) < 3:
                    logger.warning(f"[PROMO ADMIN] Неверный формат: '{text}', ожидается 'КОД СКИДКА КОЛИЧЕСТВО' (минимум 3 части через пробел)")
                    try:
                        chat_info = bot.get_chat(message.chat.id)
                        is_private = chat_info.type == 'private'
                    except:
                        is_private = message.chat.id > 0
                    
                    if is_private:
                        bot.send_message(message.chat.id, "❌ Неверный формат. Используйте: КОД СКИДКА КОЛИЧЕСТВО\nПример: NEW2026 20% 100")
                    else:
                        bot.reply_to(message, "❌ Неверный формат. Используйте: КОД СКИДКА КОЛИЧЕСТВО\nПример: NEW2026 20% 100")
                    return
                
                code = parts[0].upper()
                discount_input = parts[1]
                total_uses_str = parts[2]
                
                logger.info(f"[PROMO ADMIN] Парсинг: code='{code}', discount='{discount_input}', uses='{total_uses_str}'")
                
                from moviebot.utils.promo import create_promocode
                success, result_message = create_promocode(code, discount_input, total_uses_str)
                
                logger.info(f"[PROMO ADMIN] Результат создания: success={success}, message='{result_message}'")
                
                try:
                    chat_info = bot.get_chat(message.chat.id)
                    is_private = chat_info.type == 'private'
                except:
                    is_private = message.chat.id > 0
                
                if success:
                    # Перезагружаем список промокодов
                    from moviebot.bot.handlers.promo import promo_command
                    class FakeMessage:
                        def __init__(self, chat_id, user_id):
                            self.chat = type('obj', (object,), {'id': chat_id, 'type': 'private'})()
                            self.from_user = type('obj', (object,), {'id': user_id})()
                            self.text = '/promo'
                    
                    fake_msg = FakeMessage(chat_id, user_id)
                    promo_command(fake_msg)
                    
                    response_text = f"✅ {result_message}"
                    if is_private:
                        bot.send_message(message.chat.id, response_text)
                    else:
                        bot.reply_to(message, response_text)
                    logger.info(f"[PROMO ADMIN] ✅ Промокод создан: {code}, discount={discount_input}, uses={total_uses_str}")
                else:
                    error_text = f"❌ {result_message}"
                    if is_private:
                        bot.send_message(message.chat.id, error_text)
                    else:
                        bot.reply_to(message, error_text)
                    logger.warning(f"[PROMO ADMIN] ❌ Ошибка создания промокода: {result_message}")
                
                # НЕ удаляем состояние, так как пользователь может создать еще промокоды
                logger.info(f"[PROMO ADMIN] ✅ Завершено, состояние сохранено")
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

