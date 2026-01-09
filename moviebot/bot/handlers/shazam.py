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
        # Ищем фильмы
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
        
        # Получаем информацию о фильмах из Kinopoisk
        films_info = []
        for result in results:
            imdb_id = result['imdb_id']
            try:
                film_info = get_film_by_imdb_id(imdb_id)
                if film_info:
                    films_info.append({
                        'kp_id': film_info.get('kp_id'),
                        'title': film_info.get('title', result['title']),
                        'year': film_info.get('year', result.get('year')),
                        'imdb_id': imdb_id
                    })
            except Exception as e:
                logger.warning(f"Не удалось получить информацию о фильме {imdb_id}: {e}")
                # Используем данные из IMDB
                films_info.append({
                    'kp_id': None,
                    'title': result['title'],
                    'year': result.get('year'),
                    'imdb_id': imdb_id
                })
        
        if not films_info:
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔮 Вернуться к КиноШазаму", callback_data="shazam:start"))
            
            bot.edit_message_text(
                "❌ Не удалось получить информацию о найденных фильмах.",
                loading_msg.chat.id,
                loading_msg.message_id,
                reply_markup=markup
            )
            shazam_state.pop(user_id, None)
            return
        
        # Формируем ответ
        text = "🎬 <b>Вот наиболее подходящие фильмы по вашему описанию:</b>\n\n"
        
        markup = InlineKeyboardMarkup(row_width=1)
        for i, film in enumerate(films_info[:5], 1):
            title = film['title']
            year = f" ({film['year']})" if film.get('year') else ""
            text += f"{i}. {title}{year}\n"
            
            # Кнопка для просмотра информации о фильме
            if film.get('kp_id'):
                markup.add(InlineKeyboardButton(
                    f"{i}. {title}{year}",
                    callback_data=f"shazam:film:{film['kp_id']}"
                ))
        
        markup.add(InlineKeyboardButton("⬅️ Вернуться к Шазаму", callback_data="shazam:start"))
        
        bot.edit_message_text(
            text,
            loading_msg.chat.id,
            loading_msg.message_id,
            reply_markup=markup,
            parse_mode='HTML'
        )
        
        # Очищаем состояние
        shazam_state.pop(user_id, None)
        
    except Exception as e:
        logger.error(f"Ошибка в process_shazam_text_query: {e}", exc_info=True)
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🔮 Вернуться к КиноШазаму", callback_data="shazam:start"))
        
        bot.edit_message_text(
            "❌ Произошла ошибка при поиске. Попробуйте еще раз.",
            loading_msg.chat.id,
            loading_msg.message_id,
            reply_markup=markup
        )
        shazam_state.pop(user_id, None)


def process_shazam_voice_async(message, loading_msg):
    """Асинхронная обработка голосового сообщения Shazam"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    logger.info(f"[SHAZAM VOICE ASYNC] ===== START: user_id={user_id}, chat_id={chat_id}")
    
    try:
        # Скачиваем голосовое сообщение
        logger.info(f"[SHAZAM VOICE ASYNC] Скачиваем голосовое сообщение...")
        file_info = bot.get_file(message.voice.file_id)
        logger.info(f"[SHAZAM VOICE ASYNC] file_info получен: file_path={file_info.file_path}, file_size={file_info.file_size}")
        
        ogg_path = os.path.join(tempfile.gettempdir(), f"voice_{user_id}_{message.voice.file_id}.ogg")
        logger.info(f"[SHAZAM VOICE ASYNC] Сохраняем в {ogg_path}")
        
        downloaded_file = bot.download_file(file_info.file_path)
        with open(ogg_path, 'wb') as f:
            f.write(downloaded_file)
        logger.info(f"[SHAZAM VOICE ASYNC] Файл скачан, размер: {os.path.getsize(ogg_path)} байт")
        
        # Конвертируем в WAV
        logger.info(f"[SHAZAM VOICE ASYNC] Конвертируем OGG в WAV...")
        wav_path = os.path.join(tempfile.gettempdir(), f"voice_{user_id}_{message.voice.file_id}.wav")
        if not convert_ogg_to_wav(ogg_path, wav_path):
            logger.error(f"[SHAZAM VOICE ASYNC] Ошибка конвертации OGG в WAV")
            bot.edit_message_text(
                "❌ Ошибка конвертации аудио. Попробуйте записать еще раз.",
                loading_msg.chat.id,
                loading_msg.message_id
            )
            shazam_state.pop(user_id, None)
            try:
                os.remove(ogg_path)
            except:
                pass
            return
        
        logger.info(f"[SHAZAM VOICE ASYNC] Конвертация завершена, размер WAV: {os.path.getsize(wav_path)} байт")
        
        # Распознаем речь
        logger.info(f"[SHAZAM VOICE ASYNC] Начинаем распознавание речи...")
        text = transcribe_voice(wav_path)
        logger.info(f"[SHAZAM VOICE ASYNC] Распознавание завершено, результат: '{text}'")
        
        # Удаляем временные файлы
        try:
            os.remove(ogg_path)
            os.remove(wav_path)
        except:
            pass
        
        if not text:
            logger.warning(f"[SHAZAM VOICE ASYNC] Не удалось распознать речь")
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🔮 Вернуться к КиноШазаму", callback_data="shazam:start"))
            
            bot.edit_message_text(
                "❌ Не удалось распознать речь.\nПопробуйте записать еще раз или опишите фильм текстом.",
                loading_msg.chat.id,
                loading_msg.message_id,
                reply_markup=markup
            )
            shazam_state.pop(user_id, None)
            return
        
        # Обновляем сообщение с распознанным текстом
        logger.info(f"[SHAZAM VOICE ASYNC] Обновляем сообщение с распознанным текстом...")
        try:
            bot.edit_message_text(
                f"🎤 Распознано: <i>{text}</i>\n\n🔍 Ищем фильмы...",
                loading_msg.chat.id,
                loading_msg.message_id,
                parse_mode='HTML'
            )
            logger.info(f"[SHAZAM VOICE ASYNC] Сообщение обновлено")
        except Exception as e:
            logger.warning(f"[SHAZAM VOICE ASYNC] Не удалось обновить сообщение: {e}, продолжаем...")
        
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
        
        # Получаем информацию о фильмах из Kinopoisk
        logger.info(f"[SHAZAM VOICE ASYNC] Получаем информацию о фильмах из Kinopoisk...")
        films_info = []
        for i, result in enumerate(results, 1):
            imdb_id = result.get('imdb_id')
            logger.info(f"[SHAZAM VOICE ASYNC] Обрабатываем фильм {i}/{len(results)}: imdb_id={imdb_id}")
            try:
                film_info = get_film_by_imdb_id(imdb_id)
                if film_info:
                    logger.info(f"[SHAZAM VOICE ASYNC] Получена информация о фильме {imdb_id}: {film_info.get('title')}")
                    films_info.append({
                        'kp_id': film_info.get('kp_id'),
                        'title': film_info.get('title', result['title']),
                        'year': film_info.get('year', result.get('year')),
                        'imdb_id': imdb_id
                    })
                else:
                    logger.warning(f"[SHAZAM VOICE ASYNC] Не удалось получить информацию о фильме {imdb_id} из Kinopoisk")
                    films_info.append({
                        'kp_id': None,
                        'title': result['title'],
                        'year': result.get('year'),
                        'imdb_id': imdb_id
                    })
            except Exception as e:
                logger.warning(f"[SHAZAM VOICE ASYNC] Ошибка при получении информации о фильме {imdb_id}: {e}", exc_info=True)
                films_info.append({
                    'kp_id': None,
                    'title': result['title'],
                    'year': result.get('year'),
                    'imdb_id': imdb_id
                })
        
        if not films_info:
            bot.edit_message_text(
                "❌ Не удалось получить информацию о найденных фильмах.",
                loading_msg.chat.id,
                loading_msg.message_id
            )
            shazam_state.pop(user_id, None)
            return
        
        # Формируем ответ
        logger.info(f"[SHAZAM VOICE ASYNC] Формируем ответ с {len(films_info)} фильмами...")
        text_response = "🎬 <b>Вот наиболее подходящие фильмы по вашему описанию:</b>\n\n"
        
        markup = InlineKeyboardMarkup(row_width=1)
        for i, film in enumerate(films_info[:5], 1):
            title = film['title']
            year = f" ({film['year']})" if film.get('year') else ""
            text_response += f"{i}. {title}{year}\n"
            
            if film.get('kp_id'):
                markup.add(InlineKeyboardButton(
                    f"{i}. {title}{year}",
                    callback_data=f"shazam:film:{film['kp_id']}"
                ))
        
        markup.add(InlineKeyboardButton("⬅️ Вернуться к Шазаму", callback_data="shazam:start"))
        
        logger.info(f"[SHAZAM VOICE ASYNC] Отправляем финальное сообщение с результатами...")
        bot.edit_message_text(
            text_response,
            loading_msg.chat.id,
            loading_msg.message_id,
            reply_markup=markup,
            parse_mode='HTML'
        )
        logger.info(f"[SHAZAM VOICE ASYNC] ===== SUCCESS: Результаты отправлены пользователю")
        
        # Очищаем состояние
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

