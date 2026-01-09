from moviebot.bot.bot_init import bot
"""
Обработчики для функции КиноШазам (поиск фильмов по описанию)
"""
import logging
import os
import tempfile
from threading import Thread
from moviebot.bot.bot_init import BOT_ID
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton


from moviebot.services.shazam_service import (

    search_movies,
    transcribe_voice,
    convert_ogg_to_wav
)
from moviebot.api.kinopoisk_api import get_film_by_imdb_id

from moviebot.utils.helpers import has_recommendations_access

from moviebot.states import shazam_state

from moviebot.bot.handlers.text_messages import expect_text_from_user


logger = logging.getLogger(__name__)


def process_shazam_text_query(message, query, reply_to_message=None):
    """Единая логика обработки текстового запроса Shazam. Используется обоими обработчиками."""
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Проверяем доступ
    if not has_recommendations_access(chat_id, user_id):
        if reply_to_message:
            bot.reply_to(message, "❌ Нет доступа к этой функции")
        else:
            bot.send_message(chat_id, "❌ Нет доступа к этой функции")
        shazam_state.pop(user_id, None)
        return
    
    # Показываем анимацию загрузки
    if reply_to_message:
        loading_msg = bot.reply_to(message, "🔍 Мы уже ищем что-то похожее...")
    else:
        loading_msg = bot.send_message(chat_id, "🔍 Мы уже ищем что-то похожее...")
    
    try:
        # Ищем фильмы — теперь results уже с данными OMDB
        results = search_movies(query, top_k=5)
        
        if not results:
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔮 Вернуться к КиноШазаму", callback_data="shazam:start"))
            
            bot.edit_message_text(
                "❌ Не удалось найти подходящие фильмы.\nПопробуйте описать по-другому.",
                loading_msg.chat.id,
                loading_msg.message_id,
                reply_markup=markup
            )
            shazam_state.pop(user_id, None)
            return
        
        # Удаляем сообщение "ищем..."
        try:
            bot.delete_message(loading_msg.chat.id, loading_msg.message_id)
        except:
            pass
        
        # Кнопки соберём отдельно
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Отправляем каждый фильм карточкой с постером
        for i, result in enumerate(results[:5], 1):
            title = result['title']
            year = f" ({result['year']})" if result.get('year') else ""
            director = result.get('director', '')
            actors = result.get('actors', '')
            rating = result.get('imdb_rating', '')
            poster_url = result.get('poster_url')
            
            card_text = f"<b>{i}. {title}{year}</b>\n"
            if director and director != "Не указано":
                card_text += f"🎬 Режиссёр: {director}\n"
            if actors and actors != "Не указано":
                card_text += f"🎭 В ролях: {actors}\n"
            if rating and rating != "N/A":
                card_text += f"⭐ IMDb: {rating}\n"
            
            # Пробуем взять kp_id для кнопки "Подробнее"
            kp_id = None
            imdb_id = result['imdb_id']
            try:
                film_info = get_film_by_imdb_id(imdb_id)
                if film_info and film_info.get('kp_id'):
                    kp_id = film_info['kp_id']
            except Exception as e:
                logger.warning(f"Kinopoisk не дал kp_id для {imdb_id}: {e}")
            
            # Кнопка
            button_text = f"Подробнее о {i}. {title}{year}"
            if kp_id:
                markup.add(InlineKeyboardButton(button_text, callback_data=f"shazam:film:{kp_id}"))
            else:
                markup.add(InlineKeyboardButton(button_text, callback_data="shazam:no_kp"))
            
            # Отправляем с постером или без
            if poster_url:
                try:
                    bot.send_photo(
                        chat_id=chat_id,
                        photo=poster_url,
                        caption=card_text,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logger.warning(f"Не удалось отправить постер: {e}")
                    bot.send_message(chat_id=chat_id, text=card_text, parse_mode='HTML')
            else:
                bot.send_message(chat_id=chat_id, text=card_text, parse_mode='HTML')
        
        # Кнопка возврата
        markup.add(InlineKeyboardButton("⬅️ Вернуться к Шазаму", callback_data="shazam:start"))
        
        # Финальное сообщение с кнопками
        bot.send_message(
            chat_id=chat_id,
            text="👆 Выберите фильм для подробной информации на Кинопоиске:",
            reply_markup=markup
        )
        
        shazam_state.pop(user_id, None)
        
    except Exception as e:
        logger.error(f"Ошибка в process_shazam_text_query: {e}", exc_info=True)
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🔮 Вернуться к КиноШазаму", callback_data="shazam:start"))
        
        try:
            bot.edit_message_text(
                "❌ Произошла ошибка при поиске. Попробуйте еще раз.",
                loading_msg.chat.id,
                loading_msg.message_id,
                reply_markup=markup
            )
        except:
            pass
        shazam_state.pop(user_id, None)


def process_shazam_voice_async(message, loading_msg):
    """Асинхронная обработка голосового сообщения Shazam"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    logger.info(f"[SHAZAM VOICE ASYNC] ===== START: user_id={user_id}, chat_id={chat_id}")
    
    try:
        # ... (всё до поиска фильмов остаётся без изменений) ...
        # (скачивание, конвертация, распознавание — не трогаем)
        
        # После успешного распознавания текста и до поиска — всё как было
        
        # Ищем фильмы
        logger.info(f"[SHAZAM VOICE ASYNC] Начинаем поиск фильмов по запросу: '{text}'")
        results = search_movies(text, top_k=5)
        logger.info(f"[SHAZAM VOICE ASYNC] Поиск завершен, найдено результатов: {len(results)}")
        
        if not results:
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔮 Вернуться к КиноШазаму", callback_data="shazam:start"))
            
            bot.edit_message_text(
                "❌ Не удалось найти подходящие фильмы.\nПопробуйте описать по-другому.",
                loading_msg.chat.id,
                loading_msg.message_id,
                reply_markup=markup
            )
            shazam_state.pop(user_id, None)
            return
        
        # Удаляем сообщение "ищем..."
        try:
            bot.delete_message(loading_msg.chat.id, loading_msg.message_id)
        except:
            pass
        
        # Кнопки
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Отправляем карточки
        for i, result in enumerate(results[:5], 1):
            title = result['title']
            year = f" ({result['year']})" if result.get('year') else ""
            director = result.get('director', '')
            actors = result.get('actors', '')
            rating = result.get('imdb_rating', '')
            poster_url = result.get('poster_url')
            
            card_text = f"<b>{i}. {title}{year}</b>\n"
            if director and director != "Не указано":
                card_text += f"🎬 Режиссёр: {director}\n"
            if actors and actors != "Не указано":
                card_text += f"🎭 В ролях: {actors}\n"
            if rating and rating != "N/A":
                card_text += f"⭐ IMDb: {rating}\n"
            
            # kp_id для кнопки
            kp_id = None
            imdb_id = result['imdb_id']
            try:
                film_info = get_film_by_imdb_id(imdb_id)
                if film_info and film_info.get('kp_id'):
                    kp_id = film_info['kp_id']
            except Exception as e:
                logger.warning(f"Kinopoisk не дал kp_id для {imdb_id}: {e}")
            
            button_text = f"Подробнее о {i}. {title}{year}"
            if kp_id:
                markup.add(InlineKeyboardButton(button_text, callback_data=f"shazam:film:{kp_id}"))
            else:
                markup.add(InlineKeyboardButton(button_text, callback_data="shazam:no_kp"))
            
            # Постер или текст
            if poster_url:
                try:
                    bot.send_photo(
                        chat_id=chat_id,
                        photo=poster_url,
                        caption=card_text,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    logger.warning(f"Не удалось отправить постер: {e}")
                    bot.send_message(chat_id=chat_id, text=card_text, parse_mode='HTML')
            else:
                bot.send_message(chat_id=chat_id, text=card_text, parse_mode='HTML')
        
        markup.add(InlineKeyboardButton("⬅️ Вернуться к Шазаму", callback_data="shazam:start"))
        
        bot.send_message(
            chat_id=chat_id,
            text="👆 Выберите фильм для подробной информации на Кинопоиске:",
            reply_markup=markup
        )
        
        logger.info(f"[SHAZAM VOICE ASYNC] ===== SUCCESS: Результаты отправлены пользователю")
        shazam_state.pop(user_id, None)
        
    except Exception as e:
        logger.error(f"[SHAZAM VOICE ASYNC] ===== ERROR: {e}", exc_info=True)
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🔮 Вернуться к КиноШазаму", callback_data="shazam:start"))
        
        try:
            bot.edit_message_text(
                "❌ Произошла ошибка при обработке голосового.\nПопробуйте еще раз.",
                loading_msg.chat.id,
                loading_msg.message_id,
                reply_markup=markup
            )
        except:
            try:
                bot.reply_to(message, "❌ Произошла ошибка при обработке голосового.")
            except:
                pass
        shazam_state.pop(user_id, None)

def register_shazam_handlers(bot):
    """Регистрирует обработчики для КиноШазам"""
    
    @bot.callback_query_handler(func=lambda call: call.data == "shazam:start")
    def shazam_start_callback(call):
        """Обработчик кнопки КиноШазам из главного меню"""
        try:
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Проверяем доступ
            if not has_recommendations_access(chat_id, user_id):
                text = "🔒 <b>КиноШазам</b>\n\n"
                text += "Эта функция доступна только с подпиской <b>\"Рекомендации\"</b> или <b>\"Полная\"</b>.\n\n"
                text += "Используйте /payment для оформления подписки."
                
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("💳 К подписке", callback_data="payment:tariffs:personal"))
                markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                
                bot.edit_message_text(
                    text,
                    chat_id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                return
            
            text = "🔮 <b>КиноШазам</b>\n\n"
            text += "Мы найдем для вас любой фильм, опишите его или расскажите о нем"
            
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("✍️ Написать", callback_data="shazam:text"))
            markup.add(InlineKeyboardButton("▶️ Записать голосовое", callback_data="shazam:voice"))
            markup.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_to_start_menu"))
            
            bot.edit_message_text(
                text,
                chat_id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
            
            # Устанавливаем состояние
            shazam_state[user_id] = {'mode': None, 'chat_id': chat_id}
            
        except Exception as e:
            logger.error(f"Ошибка в shazam_start_callback: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
            except:
                pass
    
    @bot.callback_query_handler(func=lambda call: call.data == "shazam:text")
    def shazam_text_callback(call):
        """Обработчик кнопки 'Написать'"""
        try:
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Проверяем доступ
            if not has_recommendations_access(chat_id, user_id):
                bot.answer_callback_query(call.id, "Нет доступа", show_alert=True)
                return
            
            text = "Опишите, что есть в фильме?\n\n"
            text += "Можете указывать актеров, ситуации или общие детали (год, жанр, страна и т.д.)\n\n"
            text += "📝 <b>Важно:</b> В группах отправьте описание в ответ на это сообщение. В личке можно отправить в ответ или следующим сообщением.\n"
            text += "Максимальная длина: 300 символов."
            
            sent_msg = bot.edit_message_text(
                text,
                chat_id,
                call.message.message_id,
                parse_mode='HTML'
            )
            
            # Устанавливаем состояние ожидания текста
            shazam_state[user_id] = {'mode': 'text', 'chat_id': chat_id}
            
            # Для лички устанавливаем ожидание текста через user_expected_text
            is_private = call.message.chat.type == 'private'
            if is_private and sent_msg:
                expect_text_from_user(user_id, chat_id, expected_for='shazam_text', message_id=sent_msg.message_id)
            
        except Exception as e:
            logger.error(f"Ошибка в shazam_text_callback: {e}", exc_info=True)
    
    @bot.callback_query_handler(func=lambda call: call.data == "shazam:voice")
    def shazam_voice_callback(call):
        """Обработчик кнопки 'Записать голосовое'"""
        try:
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Проверяем доступ
            if not has_recommendations_access(chat_id, user_id):
                bot.answer_callback_query(call.id, "Нет доступа", show_alert=True)
                return
            
            text = "Запишите голосовое сообщение, расскажите, что за фильм вы ищете\n\n"
            text += "📝 <b>Важно:</b> В группах отправьте голосовое в ответ на это сообщение. В личке можно отправить в ответ или следующим сообщением."
            
            sent_msg = bot.edit_message_text(
                text,
                chat_id,
                call.message.message_id,
                parse_mode='HTML'
            )
            
            # Устанавливаем состояние ожидания голосового
            shazam_state[user_id] = {'mode': 'voice', 'chat_id': chat_id, 'message_id': sent_msg.message_id if sent_msg else None}
            
        except Exception as e:
            logger.error(f"Ошибка в shazam_voice_callback: {e}", exc_info=True)
    
    # Обработчики текста теперь в text_messages.py (handle_expected_text_in_private и handle_group_shazam_text_reply)
    
    # ==================== ОБРАБОТЧИКИ ГОЛОСОВЫХ СООБЩЕНИЙ ====================
    
    def is_shazam_voice_in_private(message):
        """Проверка для обработчика голосового сообщения Shazam в ЛС"""
        if message.chat.type != 'private':
            return False
        user_id = message.from_user.id
        if user_id not in shazam_state:
            return False
        if shazam_state[user_id].get('mode') != 'voice':
            return False
        if not message.voice:
            return False
        return True
    
    @bot.message_handler(content_types=['voice'], func=is_shazam_voice_in_private)
    def handle_shazam_voice_in_private(message):
        """Обработчик голосового запроса Shazam в ЛС - принимает первое голосовое сообщение"""
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        logger.info(f"[SHAZAM VOICE PRIVATE] ===== START: user_id={user_id}, chat_id={chat_id}, duration={message.voice.duration if message.voice else 'N/A'}")
        
        try:
            # Проверяем доступ
            if not has_recommendations_access(chat_id, user_id):
                logger.warning(f"[SHAZAM VOICE PRIVATE] Нет доступа для user_id={user_id}")
                bot.send_message(chat_id, "❌ Нет доступа к этой функции")
                shazam_state.pop(user_id, None)
                return
            
            # Проверяем длину голосового (Telegram max 1 мин = 60 сек)
            if message.voice.duration > 60:
                logger.warning(f"[SHAZAM VOICE PRIVATE] Голосовое слишком длинное: {message.voice.duration} сек")
                bot.send_message(chat_id, "❌ Голосовое сообщение слишком длинное (максимум 1 минута)")
                shazam_state.pop(user_id, None)
                return
            
            # Показываем анимацию загрузки и запускаем асинхронную обработку
            logger.info(f"[SHAZAM VOICE PRIVATE] Отправляем сообщение о распознавании и запускаем асинхронную обработку...")
            loading_msg = bot.send_message(chat_id, "⏳ Минуту, идёт поиск")
            logger.info(f"[SHAZAM VOICE PRIVATE] Сообщение отправлено, message_id={loading_msg.message_id}, запускаем поток")
            
            # Очищаем состояние сразу, чтобы следующее голосовое не обрабатывалось
            shazam_state.pop(user_id, None)
            
            # Запускаем обработку в отдельном потоке
            thread = Thread(target=process_shazam_voice_async, args=(message, loading_msg))
            thread.daemon = True
            thread.start()
            logger.info(f"[SHAZAM VOICE PRIVATE] Асинхронная обработка запущена в потоке, основной handler завершен")
            
        except Exception as e:
            logger.error(f"[SHAZAM VOICE PRIVATE] ===== CRITICAL ERROR: {e}", exc_info=True)
            try:
                bot.send_message(chat_id, f"❌ Критическая ошибка: {str(e)[:100]}")
            except:
                pass
            shazam_state.pop(user_id, None)
    
    @bot.message_handler(content_types=['voice'], func=lambda m: m.chat.type in ['group', 'supergroup'] and
                                                                    m.reply_to_message and
                                                                    m.reply_to_message.from_user.id == BOT_ID and
                                                                    m.from_user.id in shazam_state and
                                                                    shazam_state[m.from_user.id].get('mode') == 'voice' and
                                                                    "Запишите голосовое сообщение" in (m.reply_to_message.text or ""))
    def handle_shazam_voice_in_group(message):
        """Обработчик голосового запроса Shazam в группах - только reply на сообщение бота"""
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        logger.info(f"[SHAZAM VOICE GROUP] ===== START: user_id={user_id}, chat_id={chat_id}, duration={message.voice.duration if message.voice else 'N/A'}")
        
        try:
            # Проверяем доступ
            if not has_recommendations_access(chat_id, user_id):
                logger.warning(f"[SHAZAM VOICE GROUP] Нет доступа для user_id={user_id}")
                bot.reply_to(message, "❌ Нет доступа к этой функции")
                shazam_state.pop(user_id, None)
                return
            
            # Проверяем длину голосового (Telegram max 1 мин = 60 сек)
            if message.voice.duration > 60:
                logger.warning(f"[SHAZAM VOICE GROUP] Голосовое слишком длинное: {message.voice.duration} сек")
                bot.reply_to(message, "❌ Голосовое сообщение слишком длинное (максимум 1 минута)")
                shazam_state.pop(user_id, None)
                return
            
            # Показываем анимацию загрузки и запускаем асинхронную обработку
            logger.info(f"[SHAZAM VOICE GROUP] Отправляем сообщение о распознавании и запускаем асинхронную обработку...")
            loading_msg = bot.reply_to(message, "⏳ Минуту, идёт поиск")
            logger.info(f"[SHAZAM VOICE GROUP] Сообщение отправлено, message_id={loading_msg.message_id}, запускаем поток")
            
            # Очищаем состояние сразу, чтобы следующее голосовое не обрабатывалось
            shazam_state.pop(user_id, None)
            
            # Запускаем обработку в отдельном потоке
            thread = Thread(target=process_shazam_voice_async, args=(message, loading_msg))
            thread.daemon = True
            thread.start()
            logger.info(f"[SHAZAM VOICE GROUP] Асинхронная обработка запущена в потоке, основной handler завершен")
            
        except Exception as e:
            logger.error(f"[SHAZAM VOICE GROUP] ===== CRITICAL ERROR: {e}", exc_info=True)
            try:
                bot.reply_to(message, f"❌ Критическая ошибка: {str(e)[:100]}")
            except:
                pass
            shazam_state.pop(user_id, None)
    
    @bot.callback_query_handler(func=lambda call: call.data.startswith("shazam:film:"))
    def shazam_film_callback(call):
        """Обработчик выбора фильма из результатов поиска"""
        try:
            bot.answer_callback_query(call.id)
            kp_id = call.data.split(":")[2]
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Получаем информацию о фильме
            from moviebot.api.kinopoisk_api import extract_movie_info
            from moviebot.bot.handlers.series import show_film_info_with_buttons
            
            link = f"https://kinopoisk.ru/film/{kp_id}"
            info = extract_movie_info(link)
            
            if not info:
                bot.answer_callback_query(call.id, "Не удалось получить информацию о фильме", show_alert=True)
                return
            
            # Показываем информацию о фильме
            show_film_info_with_buttons(
                chat_id=chat_id,
                user_id=user_id,
                info=info,
                link=link,
                kp_id=kp_id,
                existing=None
            )            
        except Exception as e:
            logger.error(f"Ошибка в shazam_film_callback: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
            except:
                pass
    
    @bot.callback_query_handler(func=lambda call: call.data == "shazam:back")
    def shazam_back_callback(call):
        """Обработчик кнопки 'Вернуться к Шазаму'"""
        try:
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Показываем главное меню КиноШазам
            text = "🔮 <b>КиноШазам</b>\n\n"
            text += "Мы найдем для вас любой фильм, опишите его или расскажите о нем"
            
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("✍️ Написать", callback_data="shazam:text"))
            markup.add(InlineKeyboardButton("▶️ Записать голосовое", callback_data="shazam:voice"))
            markup.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_to_start_menu"))
            
            bot.edit_message_text(
                text,
                chat_id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
            
        except Exception as e:
            logger.error(f"Ошибка в shazam_back_callback: {e}", exc_info=True)

    @bot.callback_query_handler(func=lambda call: call.data == "shazam:no_kp")
    def no_kp_handler(call):
        bot.answer_callback_query(call.id, "Информация на Кинопоиске недоступна", show_alert=True)