"""
Обработчики для КиноШазам - поиск фильмов по описанию
"""
import logging
import os
import tempfile
import requests
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from moviebot.bot.bot_init import bot
from moviebot.config import KP_TOKEN
from moviebot.services.shazam_service import search_movies, get_whisper, get_vosk, transcribe_with_vosk
from moviebot.states import private_chat_prompts, shazam_state
from moviebot.api.kinopoisk_api import extract_movie_info
from moviebot.utils.helpers import has_recommendations_access

logger = logging.getLogger(__name__)


def get_film_by_imdb_id(imdb_id):
    """
    Получает информацию о фильме по IMDB ID через Kinopoisk API
    
    Args:
        imdb_id: IMDB ID фильма (например, 'tt0219965')
    
    Returns:
        dict: информация о фильме или None
    """
    try:
        headers = {
            'X-API-KEY': KP_TOKEN,
            'Content-Type': 'application/json'
        }
        
        # Используем поиск по IMDB ID
        url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films?order=RATING&type=ALL&ratingFrom=0&ratingTo=10&yearFrom=1000&yearTo=3000&imdbId={imdb_id}&page=1"
        
        logger.info(f"[SHAZAM] Запрос к Kinopoisk API для IMDB ID: {imdb_id}")
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            if items and len(items) > 0:
                film = items[0]
                kp_id = str(film.get('kinopoiskId', ''))
                if kp_id:
                    # Получаем полную информацию о фильме
                    return extract_movie_info(f"https://kinopoisk.ru/film/{kp_id}")
        else:
            logger.warning(f"[SHAZAM] Kinopoisk API вернул статус {response.status_code} для IMDB ID {imdb_id}")
        
        return None
    except Exception as e:
        logger.error(f"[SHAZAM] Ошибка при получении фильма по IMDB ID {imdb_id}: {e}", exc_info=True)
        return None


def register_shazam_handlers(bot):
    """Регистрирует обработчики для КиноШазам"""
    
    @bot.callback_query_handler(func=lambda call: call.data == "start_menu:shazam")
    def shazam_start_callback(call):
        """Обработчик кнопки КиноШазам из главного меню"""
        try:
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            if chat_id != user_id:
                bot.send_message(chat_id, "🔮 КиноШазам доступен только в личных сообщениях с ботом.")
                return
            
            # Проверяем подписку (Рекомендации или Полная)
            if not has_recommendations_access(chat_id, user_id):
                text = "🔮 <b>КиноШазам</b>\n\n"
                text += "КиноШазам доступен только с подпиской <b>\"Рекомендации\"</b> или <b>\"Полная\"</b>.\n\n"
                text += "Используйте /payment для оформления подписки."
                
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("💳 К подписке", callback_data="payment:tariffs:personal"))
                markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                
                try:
                    bot.edit_message_text(
                        text,
                        chat_id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                except:
                    bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
                return
            
            text = "🔮 <b>Мы найдем для вас любой фильм, опишите его или расскажите о нем</b>"
            
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("✍️ Написать", callback_data="shazam:write"))
            markup.add(InlineKeyboardButton("▶️ Записать голосовое", callback_data="shazam:voice"))
            markup.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_to_start_menu"))
            
            try:
                bot.edit_message_text(
                    text,
                    chat_id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
            except:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
            
            logger.info(f"[SHAZAM] Пользователь {user_id} открыл КиноШазам")
        except Exception as e:
            logger.error(f"[SHAZAM] Ошибка в shazam_start_callback: {e}", exc_info=True)
    
    @bot.callback_query_handler(func=lambda call: call.data == "shazam:write")
    def shazam_write_callback(call):
        """Обработчик кнопки 'Написать'"""
        try:
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            if chat_id != user_id:
                return
            
            text = "✍️ <b>Опишите, что есть в фильме?</b>\n\nМожете указывать актеров, ситуации или общие детали (год, жанр, страна и т.д.)"
            
            msg = bot.send_message(chat_id, text, parse_mode='HTML')
            
            # Устанавливаем состояние ожидания текста
            shazam_state[user_id] = {'mode': 'text', 'message_id': msg.message_id}
            private_chat_prompts[user_id] = {'prompt_message_id': msg.message_id, 'handler_type': 'shazam'}
            
            logger.info(f"[SHAZAM] Пользователь {user_id} выбрал текстовый ввод")
        except Exception as e:
            logger.error(f"[SHAZAM] Ошибка в shazam_write_callback: {e}", exc_info=True)
    
    @bot.callback_query_handler(func=lambda call: call.data == "shazam:voice")
    def shazam_voice_callback(call):
        """Обработчик кнопки 'Записать голосовое'"""
        try:
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            if chat_id != user_id:
                return
            
            text = "▶️ <b>Запишите голосовое в ответном сообщении, расскажите, что за фильм вы ищете</b>"
            
            msg = bot.send_message(chat_id, text, parse_mode='HTML')
            
            # Устанавливаем состояние ожидания голосового
            shazam_state[user_id] = {'mode': 'voice', 'message_id': msg.message_id}
            private_chat_prompts[user_id] = {'prompt_message_id': msg.message_id, 'handler_type': 'shazam'}
            
            logger.info(f"[SHAZAM] Пользователь {user_id} выбрал голосовой ввод")
        except Exception as e:
            logger.error(f"[SHAZAM] Ошибка в shazam_voice_callback: {e}", exc_info=True)
    
    @bot.callback_query_handler(func=lambda call: call.data.startswith("shazam:back"))
    def shazam_back_callback(call):
        """Обработчик кнопки 'Вернуться к Шазаму'"""
        try:
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            if chat_id != user_id:
                return
            
            text = "🔮 <b>Мы найдем для вас любой фильм, опишите его или расскажите о нем</b>"
            
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("✍️ Написать", callback_data="shazam:write"))
            markup.add(InlineKeyboardButton("▶️ Записать голосовое", callback_data="shazam:voice"))
            markup.add(InlineKeyboardButton("⬅️ Назад", callback_data="back_to_start_menu"))
            
            try:
                bot.edit_message_text(
                    text,
                    chat_id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
            except:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        except Exception as e:
            logger.error(f"[SHAZAM] Ошибка в shazam_back_callback: {e}", exc_info=True)
    
    @bot.callback_query_handler(func=lambda call: call.data.startswith("shazam:film:"))
    def shazam_film_callback(call):
        """Обработчик выбора фильма из результатов поиска"""
        try:
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            if chat_id != user_id:
                return
            
            imdb_id = call.data.split(":")[2]
            
            # Получаем информацию о фильме через Kinopoisk API
            film_info = get_film_by_imdb_id(imdb_id)
            
            if film_info:
                # Используем существующую функцию для показа карточки фильма
                from moviebot.bot.handlers.series import show_film_info_without_adding
                
                # Создаем фиктивное сообщение для вызова функции
                class FakeMessage:
                    def __init__(self):
                        self.chat = type('obj', (object,), {'id': chat_id})()
                        self.from_user = type('obj', (object,), {'id': user_id})()
                        self.text = f"https://kinopoisk.ru/film/{film_info['kp_id']}"
                
                fake_msg = FakeMessage()
                show_film_info_without_adding(fake_msg, film_info)
            else:
                bot.send_message(chat_id, "❌ Не удалось найти информацию о фильме")
            
            logger.info(f"[SHAZAM] Пользователь {user_id} выбрал фильм с IMDB ID {imdb_id}")
        except Exception as e:
            logger.error(f"[SHAZAM] Ошибка в shazam_film_callback: {e}", exc_info=True)
    
    @bot.message_handler(func=lambda message: message.chat.type == 'private' and 
                         message.from_user.id in private_chat_prompts and 
                         private_chat_prompts.get(message.from_user.id, {}).get('handler_type') == 'shazam' and
                         message.text and not message.text.startswith('/'))
    def shazam_text_handler(message):
        """Обработчик текстового запроса для КиноШазам"""
        try:
            user_id = message.from_user.id
            chat_id = message.chat.id
            
            # Проверяем, что это ответ на наш prompt или следующее сообщение
            prompt_info = private_chat_prompts.get(user_id, {})
            if not prompt_info or prompt_info.get('handler_type') != 'shazam':
                return False
            
            state = shazam_state.get(user_id, {})
            if state.get('mode') != 'text':
                return False
            
            # Проверяем, что это либо реплай на наше сообщение, либо следующее сообщение после промпта
            prompt_message_id = prompt_info.get('prompt_message_id')
            if message.reply_to_message:
                # Если это реплай, проверяем, что это реплай на наш промпт
                if message.reply_to_message.message_id != prompt_message_id:
                    return False
            # Если не реплай, это должно быть следующее сообщение после промпта
            
            query = message.text.strip()
            if not query:
                bot.send_message(chat_id, "Пожалуйста, опишите фильм текстом.")
                return True
            
            # Удаляем состояние
            if user_id in shazam_state:
                del shazam_state[user_id]
            if user_id in private_chat_prompts:
                del private_chat_prompts[user_id]
            
            # Показываем анимацию загрузки
            loading_msg = bot.send_message(chat_id, "🔍 Мы уже ищем что-то похожее...")
            
            try:
                # Выполняем поиск
                results = search_movies(query, top_k=5)
                
                if not results:
                    bot.edit_message_text(
                        "❌ К сожалению, ничего не найдено. Попробуйте описать фильм по-другому.",
                        chat_id,
                        loading_msg.message_id
                    )
                    return True
                
                # Получаем информацию о фильмах через Kinopoisk API
                films_data = []
                for result in results:
                    film_info = get_film_by_imdb_id(result['imdb_id'])
                    if film_info:
                        films_data.append({
                            'imdb_id': result['imdb_id'],
                            'kp_id': film_info.get('kp_id'),
                            'title': film_info.get('title', result['title']),
                            'year': film_info.get('year', result.get('year'))
                        })
                
                if not films_data:
                    bot.edit_message_text(
                        "❌ Не удалось найти информацию о фильмах. Попробуйте еще раз.",
                        chat_id,
                        loading_msg.message_id
                    )
                    return True
                
                # Формируем сообщение с результатами
                text = "🎬 <b>Вот наиболее подходящие фильмы по вашему описанию:</b>\n\n"
                
                markup = InlineKeyboardMarkup(row_width=1)
                for i, film in enumerate(films_data[:5], 1):
                    title = film['title']
                    year = film.get('year', '')
                    year_str = f" ({year})" if year else ""
                    text += f"{i}. {title}{year_str}\n"
                    markup.add(InlineKeyboardButton(
                        f"{i}. {title}{year_str}",
                        callback_data=f"shazam:film:{film['imdb_id']}"
                    ))
                
                markup.add(InlineKeyboardButton("⬅️ Вернуться к Шазаму", callback_data="shazam:back"))
                
                bot.edit_message_text(
                    text,
                    chat_id,
                    loading_msg.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                
                logger.info(f"[SHAZAM] Пользователь {user_id} получил {len(films_data)} результатов для запроса: {query}")
            except Exception as e:
                logger.error(f"[SHAZAM] Ошибка при поиске: {e}", exc_info=True)
                bot.edit_message_text(
                    "❌ Произошла ошибка при поиске. Попробуйте еще раз.",
                    chat_id,
                    loading_msg.message_id
                )
            
            return True
        except Exception as e:
            logger.error(f"[SHAZAM] Ошибка в shazam_text_handler: {e}", exc_info=True)
            return False
    
    @bot.message_handler(content_types=['voice'], func=lambda message: 
                         message.chat.type == 'private' and 
                         message.from_user.id in private_chat_prompts and 
                         private_chat_prompts.get(message.from_user.id, {}).get('handler_type') == 'shazam')
    def shazam_voice_handler(message):
        """Обработчик голосового сообщения для КиноШазам"""
        try:
            user_id = message.from_user.id
            chat_id = message.chat.id
            
            # Проверяем, что это ответ на наш prompt или следующее сообщение
            prompt_info = private_chat_prompts.get(user_id, {})
            if not prompt_info or prompt_info.get('handler_type') != 'shazam':
                return False
            
            state = shazam_state.get(user_id, {})
            if state.get('mode') != 'voice':
                return False
            
            # Проверяем, что это либо реплай на наше сообщение, либо следующее сообщение после промпта
            prompt_message_id = prompt_info.get('prompt_message_id')
            if message.reply_to_message:
                # Если это реплай, проверяем, что это реплай на наш промпт
                if message.reply_to_message.message_id != prompt_message_id:
                    return False
            # Если не реплай, это должно быть следующее сообщение после промпта
            
            # Удаляем состояние
            if user_id in shazam_state:
                del shazam_state[user_id]
            if user_id in private_chat_prompts:
                del private_chat_prompts[user_id]
            
            # Показываем анимацию загрузки
            loading_msg = bot.send_message(chat_id, "🎤 Распознаю голосовое сообщение...")
            
            try:
                # Получаем информацию о голосовом сообщении
                voice = message.voice
                file_id = voice.file_id
                
                # Получаем file_path
                file_info = bot.get_file(file_id)
                file_path = file_info.file_path
                
                # Примечание: Telegram Bot API не поддерживает messages.transcribeAudio напрямую
                # Это метод из Telegram Client API (MTProto). Для его использования нужна библиотека
                # Telethon или Pyrogram. Пока используем Whisper как fallback.
                # В будущем можно добавить интеграцию с Telegram Client API для использования
                # нативного распознавания голоса Telegram.
                
                # Скачиваем голосовое сообщение
                file_url = f"https://api.telegram.org/file/bot{bot.token}/{file_path}"
                
                with tempfile.NamedTemporaryFile(delete=False, suffix='.ogg') as tmp_file:
                    response = requests.get(file_url, stream=True, timeout=30)
                    response.raise_for_status()
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            tmp_file.write(chunk)
                    tmp_path = tmp_file.name
                
                query = None
                wav_path = None
                try:
                    # Конвертируем OGG в WAV (нужно для обоих вариантов)
                    from pydub import AudioSegment
                    wav_path = tmp_path.replace('.ogg', '.wav')
                    
                    try:
                        # Конвертируем в WAV с правильными параметрами для Vosk (16kHz, mono)
                        audio = AudioSegment.from_ogg(tmp_path)
                        audio = audio.set_frame_rate(16000).set_channels(1)  # 16kHz, mono для Vosk
                        audio.export(wav_path, format="wav")
                    except Exception as conv_error:
                        logger.error(f"[SHAZAM] Ошибка при конвертации аудио: {conv_error}", exc_info=True)
                        # Пробуем стандартную конвертацию
                        AudioSegment.from_ogg(tmp_path).export(wav_path, format="wav")
                    
                    # Пробуем Whisper (основной вариант)
                    whisper = get_whisper()
                    if whisper:
                        try:
                            logger.info("[SHAZAM] Используем Whisper для распознавания...")
                            result = whisper(wav_path)
                            query = result.get("text", "") if isinstance(result, dict) else str(result)
                            if query and query.strip():
                                logger.info(f"[SHAZAM] Whisper распознал: {query[:50]}...")
                        except Exception as whisper_error:
                            logger.warning(f"[SHAZAM] Whisper не смог распознать: {whisper_error}")
                            query = None
                    
                    # Если Whisper не сработал, пробуем Vosk (запасной вариант)
                    if not query or not query.strip():
                        logger.info("[SHAZAM] Пробуем Vosk (запасной вариант)...")
                        query = transcribe_with_vosk(wav_path)
                        if query and query.strip():
                            logger.info(f"[SHAZAM] Vosk распознал: {query[:50]}...")
                    
                    # Очищаем временные файлы
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                    if wav_path and os.path.exists(wav_path):
                        os.remove(wav_path)
                    
                    if not query or not query.strip():
                        # Если оба варианта не сработали
                        bot.edit_message_text(
                            "❌ Не удалось распознать голосовое сообщение. Пожалуйста, опишите фильм текстом.",
                            chat_id,
                            loading_msg.message_id
                        )
                        return True
                        
                except Exception as e:
                    logger.error(f"[SHAZAM] Ошибка при распознавании голоса: {e}", exc_info=True)
                    # Очищаем временные файлы в случае ошибки
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                    if wav_path and os.path.exists(wav_path):
                        os.remove(wav_path)
                    bot.edit_message_text(
                        "❌ Ошибка при распознавании голоса. Пожалуйста, опишите фильм текстом.",
                        chat_id,
                        loading_msg.message_id
                    )
                    return True
                
                # Обновляем сообщение
                bot.edit_message_text(
                    f"🔍 Распознано: {query}\n\nМы уже ищем что-то похожее...",
                    chat_id,
                    loading_msg.message_id
                )
                
                # Выполняем поиск (такой же как в текстовом handler)
                results = search_movies(query.strip(), top_k=5)
                
                if not results:
                    bot.edit_message_text(
                        "❌ К сожалению, ничего не найдено. Попробуйте описать фильм по-другому.",
                        chat_id,
                        loading_msg.message_id
                    )
                    return True
                
                # Получаем информацию о фильмах через Kinopoisk API
                films_data = []
                for result in results:
                    film_info = get_film_by_imdb_id(result['imdb_id'])
                    if film_info:
                        films_data.append({
                            'imdb_id': result['imdb_id'],
                            'kp_id': film_info.get('kp_id'),
                            'title': film_info.get('title', result['title']),
                            'year': film_info.get('year', result.get('year'))
                        })
                
                if not films_data:
                    bot.edit_message_text(
                        "❌ Не удалось найти информацию о фильмах. Попробуйте еще раз.",
                        chat_id,
                        loading_msg.message_id
                    )
                    return True
                
                # Формируем сообщение с результатами
                text = "🎬 <b>Вот наиболее подходящие фильмы по вашему описанию:</b>\n\n"
                
                markup = InlineKeyboardMarkup(row_width=1)
                for i, film in enumerate(films_data[:5], 1):
                    title = film['title']
                    year = film.get('year', '')
                    year_str = f" ({year})" if year else ""
                    text += f"{i}. {title}{year_str}\n"
                    markup.add(InlineKeyboardButton(
                        f"{i}. {title}{year_str}",
                        callback_data=f"shazam:film:{film['imdb_id']}"
                    ))
                
                markup.add(InlineKeyboardButton("⬅️ Вернуться к Шазаму", callback_data="shazam:back"))
                
                bot.edit_message_text(
                    text,
                    chat_id,
                    loading_msg.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                
                logger.info(f"[SHAZAM] Пользователь {user_id} получил {len(films_data)} результатов для голосового запроса")
            except Exception as e:
                logger.error(f"[SHAZAM] Ошибка при обработке голосового: {e}", exc_info=True)
                bot.edit_message_text(
                    "❌ Произошла ошибка при обработке голосового сообщения. Попробуйте еще раз.",
                    chat_id,
                    loading_msg.message_id
                )
            
            return True
        except Exception as e:
            logger.error(f"[SHAZAM] Ошибка в shazam_voice_handler: {e}", exc_info=True)
            return False

