"""
Обработчики команд связанных с сериалами, поиском, рандомом, премьерами, билетами, настройками и помощью
"""
import logging
import re
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request, get_user_timezone_or_default, set_user_timezone
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.api.kinopoisk_api import search_films, extract_movie_info, get_premieres_for_period, get_seasons_data
from moviebot.utils.helpers import has_tickets_access, has_recommendations_access, has_notifications_access
from moviebot.bot.handlers.seasons import get_series_airing_status, count_episodes_for_watch_check
from moviebot.bot.bot_init import bot as bot_instance
from moviebot.config import KP_TOKEN
import requests
from moviebot.states import (
    user_search_state, user_random_state, user_ticket_state,
    user_settings_state, settings_messages, bot_messages, added_movie_messages
)
from moviebot.utils.parsing import extract_kp_id_from_text
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


# Вспомогательная функция для поиска с фильтрацией по типу
def search_films_with_type(query, page=1, search_type='mixed'):
    """
    Поиск фильмов с фильтрацией по типу
    Использует фильтрацию на стороне клиента, так как API не поддерживает фильтрацию по типу
    """
    films, total_pages = search_films(query, page)
    
    if search_type == 'film':
        # Фильтруем только фильмы
        films = [f for f in films if f.get('type', '').upper() != 'TV_SERIES']
    elif search_type == 'series':
        # Фильтруем только сериалы
        films = [f for f in films if f.get('type', '').upper() == 'TV_SERIES']
    # Если search_type == 'mixed', возвращаем все
    
    return films, total_pages


def handle_search(message):
    """Команда /search - поиск фильмов и сериалов"""
    logger.info(f"[HANDLER] /search вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/search', message.chat.id)
        
        query = message.text.split(maxsplit=1)[1] if len(message.text.split()) > 1 else None
        if not query:
            # Создаем кнопки для выбора типа поиска
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(
                InlineKeyboardButton("🎬 Найти фильм", callback_data="search_type:film"),
                InlineKeyboardButton("📺 Найти сериал", callback_data="search_type:series")
            )
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            reply_msg = bot_instance.reply_to(message, "🔍 Укажите запрос для поиска в ответном сообщении, например: джон уик", reply_markup=markup)
            # Сохраняем состояние для получения запроса (по умолчанию смешанный поиск)
            user_search_state[message.from_user.id] = {'chat_id': message.chat.id, 'message_id': reply_msg.message_id, 'search_type': 'mixed'}
            return
        
        logger.info(f"Команда /search от пользователя {message.from_user.id}, запрос: {query}")
        
        # Получаем тип поиска из состояния, если есть
        search_type = user_search_state.get(message.from_user.id, {}).get('search_type', 'mixed')
        films, total_pages = search_films_with_type(query, page=1, search_type=search_type)
        if not films:
            bot_instance.reply_to(message, f"❌ Ничего не найдено по запросу '{query}'")
            return
        
        # Формируем сообщение с результатами
        results_text = f"🔍 Результаты поиска '{query}':\n\n"
        markup = InlineKeyboardMarkup(row_width=1)
        
        for film in films[:10]:  # Показываем максимум 10 результатов на странице
            # Пробуем разные варианты полей для совместимости с разными версиями API
            title = film.get('nameRu') or film.get('nameEn') or film.get('title') or "Без названия"
            year = film.get('year') or film.get('releaseYear') or 'N/A'
            rating = film.get('ratingKinopoisk') or film.get('rating') or film.get('ratingImdb') or 'N/A'
            # Пробуем разные варианты ID
            kp_id = film.get('kinopoiskId') or film.get('filmId') or film.get('id')
            
            # Определяем тип (сериал или фильм) по полю type из API
            film_type = film.get('type', '').upper()  # "FILM" или "TV_SERIES"
            is_series = film_type == 'TV_SERIES'
            
            logger.info(f"[SEARCH] Фильм: title={title}, year={year}, kp_id={kp_id}, type={film_type}, is_series={is_series}")
            
            if kp_id:
                # Ограничиваем длину текста кнопки
                type_indicator = "📺" if is_series else "🎬"
                button_text = f"{type_indicator} {title} ({year})"
                if len(button_text) > 50:
                    button_text = button_text[:47] + "..."
                results_text += f"• {type_indicator} <b>{title}</b> ({year})"
                if rating != 'N/A':
                    results_text += f" ⭐ {rating}"
                results_text += "\n"
                # Сохраняем тип в callback_data для правильного формирования ссылки
                markup.add(InlineKeyboardButton(button_text, callback_data=f"add_film_{kp_id}:{film_type}"))
            else:
                logger.warning(f"[SEARCH] Фильм без ID: {film}")
        
        # Добавляем пагинацию, если нужно
        if total_pages > 1:
            pagination_row = []
            # Кодируем запрос для callback_data (заменяем пробелы на подчеркивания)
            query_encoded = query.replace(' ', '_')
            pagination_row.append(InlineKeyboardButton(f"Страница 1/{total_pages}", callback_data="noop"))
            if total_pages > 1:
                pagination_row.append(InlineKeyboardButton("Далее ▶️", callback_data=f"search_{query_encoded}_2"))
            markup.row(*pagination_row)
        
        # Добавляем кнопку "Назад в меню"
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
        # Добавляем пояснение про эмодзи
        results_text += "\n\n🎬 - фильм\n📺 - сериал"
        
        results_msg = bot_instance.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
        # Сохраняем message_id результатов поиска для кнопки "Назад"
        if results_msg:
            user_search_state[message.from_user.id] = {
                'chat_id': message.chat.id,
                'message_id': results_msg.message_id,
                'search_type': search_type,
                'query': query,
                'results_text': results_text,
                'films': films[:10],  # Сохраняем первые 10 фильмов для восстановления
                'total_pages': total_pages
            }
        logger.info(f"✅ Ответ на /search отправлен пользователю {message.from_user.id}, найдено {len(films)} результатов")
    except Exception as e:
        logger.error(f"❌ Ошибка в /search: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "Произошла ошибка при обработке команды /search")
        except:
            pass


def random_start(message):
    """Команда /random - рандомный выбор фильма"""
    # TODO: Извлечь из moviebot.py строки 10210-10296
    try:
        logger.info(f"[RANDOM] ===== START: user_id={message.from_user.id}, chat_id={message.chat.id}")
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(user_id, username, '/random', chat_id)
        
        # Инициализируем состояние
        user_random_state[user_id] = {
            'step': 'mode',
            'mode': None,  # 'my_votes', 'group_votes', или None (обычный режим)
            'periods': [],
            'genres': [],
            'directors': [],
            'actors': []
        }
        
        # Шаг 0: Выбор режима
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("🎲 Рандом по своей базе", callback_data="rand_mode:database"))
        
        # Проверяем доступ к рекомендациям
        has_rec_access = has_recommendations_access(chat_id, user_id)
        
        if has_rec_access:
            markup.add(InlineKeyboardButton("🎬 Рандом по кинопоиску", callback_data="rand_mode:kinopoisk"))
        else:
            markup.add(InlineKeyboardButton("🔒 Рандом по кинопоиску", callback_data="rand_mode_locked:kinopoisk"))
        
        # TODO: Добавить проверку количества оценок и групповых оценок
        # Проверяем, есть ли у пользователя больше 50 оценок
        with db_lock:
            cursor.execute('SELECT COUNT(*) FROM ratings WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
            user_ratings_count = cursor.fetchone()
            user_ratings = user_ratings_count.get('count') if isinstance(user_ratings_count, dict) else (user_ratings_count[0] if user_ratings_count else 0)
            
            if has_rec_access and user_ratings >= 50:
                markup.add(InlineKeyboardButton("⭐ По моим оценкам (9-10)", callback_data="rand_mode:my_votes"))
            else:
                if not has_rec_access:
                    markup.add(InlineKeyboardButton("🔒 По моим оценкам (9-10)", callback_data="rand_mode_locked:my_votes"))
                else:
                    markup.add(InlineKeyboardButton("🔒 Откроется от 50 оценок с КП", callback_data="rand_mode_locked:my_votes"))
        
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        bot_instance.reply_to(message, "🎲 <b>Выберите режим рандома:</b>", reply_markup=markup, parse_mode='HTML')
        logger.info(f"✅ Ответ на /random отправлен пользователю {user_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в /random: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "Произошла ошибка при обработке команды /random")
        except:
            pass


def premieres_command(message):
    """Команда /premieres - премьеры фильмов"""
    logger.info(f"[HANDLER] /premieres вызван от {message.from_user.id}")
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/premieres', message.chat.id)
    
    # Показываем выбор периода
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("📅 Текущий месяц", callback_data="premieres_period:current_month"))
    markup.add(InlineKeyboardButton("📅 Следующий месяц", callback_data="premieres_period:next_month"))
    markup.add(InlineKeyboardButton("📅 3 месяца", callback_data="premieres_period:3_months"))
    markup.add(InlineKeyboardButton("📅 6 месяцев", callback_data="premieres_period:6_months"))
    markup.add(InlineKeyboardButton("📅 Текущий год", callback_data="premieres_period:current_year"))
    markup.add(InlineKeyboardButton("📅 Ближайший год", callback_data="premieres_period:next_year"))
    markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
    
    bot_instance.reply_to(message, "📅 <b>Выберите период для просмотра премьер:</b>", reply_markup=markup, parse_mode='HTML')


def ticket_command(message):
    """Команда /ticket - работа с билетами"""
    # TODO: Извлечь из moviebot.py строки 17031-17333
    logger.info(f"[TICKET COMMAND] ===== ФУНКЦИЯ ВЫЗВАНА =====")
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        username = message.from_user.username or f"user_{user_id}"
        log_request(user_id, username, '/ticket', chat_id)
        
        # Проверяем доступ к функциям билетов
        if not has_tickets_access(chat_id, user_id):
            text = "🎫 <b>Билеты в кино</b>\n\n"
            text += "Вы можете загружать билеты и получать их в боте прямо перед сеансом с подпиской <b>\"Билеты\"</b>.\n\n"
            text += "Используйте /payment для оформления подписки."
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🎫 К подписке Билеты", callback_data="payment:tariffs:personal"))
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            
            bot_instance.reply_to(message, text, reply_markup=markup, parse_mode='HTML')
            return
        
        # Проверяем, есть ли файл в сообщении
        has_photo = message.photo is not None and len(message.photo) > 0
        has_document = message.document is not None
        
        if has_photo or has_document:
            # Сохраняем file_id для последующей обработки
            if has_photo:
                file_id = message.photo[-1].file_id  # Берем самое большое фото
            else:
                file_id = message.document.file_id
            
            user_ticket_state[user_id] = {
                'step': 'select_session',
                'file_id': file_id,
                'chat_id': chat_id
            }
            
            # Показываем список сеансов в кино
            show_cinema_sessions(chat_id, user_id, file_id)
        else:
            # Нет файла - показываем список сеансов для выбора или сообщение об отсутствии билетов
            show_cinema_sessions(chat_id, user_id, None)
    except Exception as e:
        logger.error(f"❌ Ошибка в /ticket: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "Произошла ошибка при обработке команды /ticket")
        except:
            pass


def settings_command(message):
    """Команда /settings - настройки"""
    # TODO: Извлечь из moviebot.py строки 10627-10992
    logger.info(f"[HANDLER] /settings вызван от {message.from_user.id}")
    try:
        chat_id = message.chat.id
        user_id = message.from_user.id
        username = message.from_user.username or f"user_{user_id}"
        log_request(user_id, username, '/settings', chat_id)
        logger.info(f"Команда /settings от пользователя {user_id}")
        
        # Проверяем на reset
        if message.text and 'reset' in message.text.lower():
            with db_lock:
                cursor.execute("DELETE FROM settings WHERE chat_id = %s AND key = 'watched_emoji'", (chat_id,))
                conn.commit()
            bot_instance.reply_to(message, "✅ Реакции сброшены к значению по умолчанию (✅)")
            logger.info(f"Реакции сброшены для чата {chat_id}")
            return
        
        # Сначала показываем меню выбора действия
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("😀 Настроить эмодзи просмотра", callback_data="settings:emoji"))
        markup.add(InlineKeyboardButton("🕐 Выбрать часовой пояс", callback_data="settings:timezone"))
        
        # Проверяем доступ к настройкам напоминаний (требуется подписка на уведомления)
        if has_notifications_access(chat_id, user_id):
            markup.add(InlineKeyboardButton("⏰ Настройки напоминаний", callback_data="settings:notifications"))
        else:
            markup.add(InlineKeyboardButton("🔒 Настройки напоминаний", callback_data="settings:notifications_locked"))
        
        # Проверяем доступ к импорту базы (требуется подписка на рекомендации)
        if has_recommendations_access(chat_id, user_id):
            markup.add(InlineKeyboardButton("📥 Импорт базы из Кинопоиска", callback_data="settings:import"))
        else:
            markup.add(InlineKeyboardButton("🔒 Импорт базы из Кинопоиска", callback_data="settings:import_locked"))
        
        # Проверяем, является ли чат личным (случайные события доступны только в группах)
        is_private = message.chat.type == 'private'
        if is_private:
            markup.add(InlineKeyboardButton("🔒 Случайные события", callback_data="settings:random_events_locked"))
        else:
            markup.add(InlineKeyboardButton("🎲 Случайные события", callback_data="settings:random_events"))
        markup.add(InlineKeyboardButton("✏️ Редактировать записи", callback_data="settings:edit"))
        markup.add(InlineKeyboardButton("🗑️ Очистка базы", callback_data="settings:clean"))
        markup.add(InlineKeyboardButton("👥 Участие", callback_data="settings:join"))
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
        sent = bot_instance.send_message(chat_id,
            f"⚙️ <b>Настройки</b>\n\n"
            f"Выберите, что хотите настроить:",
            reply_markup=markup,
            parse_mode='HTML')
        
        logger.info(f"Настройки открыты для {user_id}, msg_id: {sent.message_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в /settings: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "Произошла ошибка при обработке команды /settings")
        except:
            pass


def help_command(message):
    """Команда /help - помощь"""
    logger.info(f"[HANDLER] /help вызван от {message.from_user.id}")
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/help', message.chat.id)
    logger.info(f"Команда /help от пользователя {message.from_user.id}")
    text = """🎬 Помощь по командам бота:

/list — Показать список непросмотренных фильмов
/random — Выбрать случайный непросмотренный фильм с фильтрами (год, жанр, режиссёр)
/search — Поиск фильмов через Kinopoisk API
/total — Статистика: фильмы, жанры, режиссёры, актёры, оценки
/stats — Детальная статистика группы и участников
/rate — Оценить просмотренные фильмы
/plan — Запланировать просмотр фильма (дома/в кино)
/schedule — Показать список запланированных просмотров
/settings — Настроить эмодзи для отметки просмотренных фильмов
/clean — Удалить оценку, просмотр, план или обнулить базу
/help — Эта справка

Как использовать бота:

Есть два варианта, использование лично или использование в группе. Чтобы бот работал в группе, нужно добавить бота и сделать админом группы. В боте могут участвовать не все члены группы: для того, чтобы начать участие, нужно отправить любую команду боту. Вы можете добавить других членов группы к участию в боте по команде /join.

Сценарии работы с ботом:

1) Добавление фильмов
1. Отправьте ссылку на фильм с Кинопоиска — бот автоматически добавит его
2. Запланируйте просмотр фильма — дома или в кино. При домашнем просмотре, будут предложены онлайн-кинотеатры, где можно посмотреть фильм.
3. В день просмотра вам придет напоминание о просмотре со ссылкой на кинотеатр, если смотрите дома, или с билетами, если вы подгрузили билет в кино.
4. После просмотра, поставьте реакцию на сообщение с фильмом — фильм будет отмечен как просмотренный
5. После отметки напишите оценку от 1 до 10

При групповом участии, учитываются оценки всех участников. К высоко оцененным фильмам предлагаются похожие, а также оцененные фильмы участвуют в рекомендательных функциях

2) Сериалы
Можно добавлять сериалы, трекать просмотренные серии и подписаться на уведомления

3) Планирование премьер
Если фильм ещё не вышел, вы можете подписаться на его дату выхода

4) Поиск
Вы можете искать фильмы и сериалы с командой /search, а также искать премьеры по /premiere, там будет актуальный список премьер

5) Планирование походов в кино
Вы можете запланировать, хотите вы посмотреть тот или иной фильм дома или в кино. При просмотре фильма дома, вам будут предложны онлайн-кинотеатры, а при просмотре в кино — предложена возможность загрузить билет и указать время сеанса. В день просмотра фильма придет уведомление и напоминание с билетами заранее (функционал платный). Время уведомлений можно настроить.

Приятного просмотра! 🍿

Если у вас возникли сложности с ботом или оплатой, напишите нам:
@zap_nikita
movie-planner-bot@yandex.com"""
    
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
    # Переключаемся на HTML, так как Markdown может вызывать ошибки парсинга
    text_html = text.replace('*', '').replace('_', '')
    # Добавляем базовое форматирование
    text_html = text_html.replace('🎬 Помощь по командам бота:', '<b>🎬 Помощь по командам бота:</b>')
    text_html = text_html.replace('Как использовать бота:', '<b>Как использовать бота:</b>')
    text_html = text_html.replace('Сценарии работы с ботом:', '<b>Сценарии работы с ботом:</b>')
    text_html = text_html.replace('1) Добавление фильмов', '<b>1) Добавление фильмов</b>')
    text_html = text_html.replace('2) Сериалы', '<b>2) Сериалы</b>')
    text_html = text_html.replace('3) Планирование премьер', '<b>3) Планирование премьер</b>')
    text_html = text_html.replace('4) Поиск', '<b>4) Поиск</b>')
    text_html = text_html.replace('5) Планирование походов в кино', '<b>5) Планирование походов в кино</b>')
    text_html = text_html.replace('Приятного просмотра!', '<b>Приятного просмотра!</b>')
    text_html = text_html.replace('Если у вас возникли сложности с ботом или оплатой, напишите нам:', '<b>Если у вас возникли сложности с ботом или оплатой, напишите нам:</b>')
    bot_instance.reply_to(message, text_html, reply_markup=markup, parse_mode='HTML')


def register_series_handlers(bot_instance):
    """Регистрирует обработчики команд связанных с сериалами"""
    
    @bot_instance.message_handler(commands=['search'])
    def _handle_search_handler(message):
        """Обертка для регистрации команды /search"""
        handle_search(message)
    
    @bot_instance.message_handler(commands=['random'])
    def _random_start_handler(message):
        """Обертка для регистрации команды /random"""
        random_start(message)
    
    @bot_instance.message_handler(commands=['premieres'])
    def _premieres_command_handler(message):
        """Обертка для регистрации команды /premieres"""
        premieres_command(message)
    
    @bot_instance.message_handler(commands=['ticket'])
    def _ticket_command_handler(message):
        """Обертка для регистрации команды /ticket"""
        ticket_command(message)
    
    @bot_instance.message_handler(commands=['settings'])
    def _settings_command_handler(message):
        """Обертка для регистрации команды /settings"""
        settings_command(message)
    
    @bot_instance.message_handler(commands=['help'])
    def _help_command_handler(message):
        """Обертка для регистрации команды /help"""
        help_command(message)

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("timezone:"))
    def handle_timezone_callback(call):
        """Обработчик выбора часового пояса"""
        try:
            bot_instance.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            timezone_name = call.data.split(":", 1)[1]  # "Moscow" или "Serbia"
            
            if set_user_timezone(user_id, timezone_name):
                tz_display = "Москва" if timezone_name == "Moscow" else "Сербия"
                tz_obj = pytz.timezone('Europe/Moscow' if timezone_name == "Moscow" else 'Europe/Belgrade')
                current_time = datetime.now(tz_obj).strftime('%H:%M')
                
                bot_instance.edit_message_text(
                    f"✅ Часовой пояс установлен: <b>{tz_display}</b>\n\n"
                    f"Текущее время: <b>{current_time}</b>\n\n"
                    f"Все время будет отображаться и планироваться в часовом поясе {tz_display}.\n"
                    f"Часовой пояс будет автоматически обновляться при путешествиях.",
                    call.message.chat.id,
                    call.message.message_id,
                    parse_mode='HTML'
                )
                logger.info(f"Часовой пояс установлен для user_id={user_id}: {timezone_name}")
                
                # Проверяем, есть ли сохраненный текст для продолжения планирования
                from moviebot.states import user_plan_state
                if user_id in user_plan_state:
                    state = user_plan_state[user_id]
                    pending_text = state.get('pending_text')
                    if pending_text:
                        logger.info(f"[TIMEZONE CALLBACK] Продолжаем планирование с сохраненным текстом: '{pending_text}'")
                        # Продолжаем планирование с сохраненными данными
                        link = state.get('link')
                        plan_type = state.get('type')
                        pending_plan_dt = state.get('pending_plan_dt')
                        pending_message_date_utc = state.get('pending_message_date_utc')
                        chat_id_from_state = state.get('chat_id', chat_id)
                        
                        if link and plan_type and pending_plan_dt:
                            # Импортируем process_plan из handlers/plan
                            from moviebot.bot.handlers.plan import process_plan
                            # Вызываем process_plan с сохраненными данными
                            result = process_plan(bot_instance, user_id, chat_id_from_state, link, plan_type, pending_plan_dt, pending_message_date_utc)
                            if result:
                                # Очищаем сохраненные данные
                                if 'pending_text' in state:
                                    del state['pending_text']
                                if 'pending_plan_dt' in state:
                                    del state['pending_plan_dt']
                                if 'pending_message_date_utc' in state:
                                    del state['pending_message_date_utc']
                                del user_plan_state[user_id]
                                logger.info(f"[TIMEZONE CALLBACK] Планирование успешно завершено")
                            else:
                                logger.warning(f"[TIMEZONE CALLBACK] Ошибка при продолжении планирования")
                        else:
                            logger.warning(f"[TIMEZONE CALLBACK] Недостаточно данных для продолжения планирования: link={link}, plan_type={plan_type}, pending_plan_dt={pending_plan_dt}")
            else:
                bot_instance.answer_callback_query(call.id, "Ошибка сохранения часового пояса", show_alert=True)
        except Exception as e:
            logger.error(f"[SETTINGS] Ошибка в handle_timezone_callback: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
            except:
                pass

    @bot_instance.message_handler(content_types=['text'], func=lambda m: m.text and not m.text.strip().startswith('/') and ('kinopoisk.ru' in m.text or 'kinopoisk.com' in m.text))
    def handle_kinopoisk_link(message):
        """Обработчик текстовых сообщений со ссылками на Кинопоиск"""
        try:
            user_id = message.from_user.id
            chat_id = message.chat.id
            text = message.text.strip()
            
            logger.info(f"[KINOPOISK LINK] Получена ссылка от {user_id}: {text[:100]}")
            
            # Ищем ссылки на Кинопоиск
            links = re.findall(r'(https?://[\w\./-]*(?:kinopoisk\.ru|kinopoisk\.com)/(?:film|series)/\d+)', text)
            if not links:
                return
            
            # Обрабатываем первую ссылку
            link = links[0]
            logger.info(f"[KINOPOISK LINK] Обработка ссылки: {link}")
            
            # Извлекаем kp_id
            kp_id = extract_kp_id_from_text(link)
            if not kp_id:
                logger.warning(f"[KINOPOISK LINK] Не удалось извлечь kp_id из ссылки: {link}")
                return
            
            # Получаем информацию о фильме/сериале
            info = extract_movie_info(link)
            if not info:
                logger.warning(f"[KINOPOISK LINK] Не удалось получить информацию о фильме: {link}")
                bot_instance.reply_to(message, "❌ Не удалось получить информацию о фильме/сериале.")
                return
            
            is_series = info.get('is_series', False)
            
            # Проверяем, есть ли уже в базе
            with db_lock:
                cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                row = cursor.fetchone()
                if row:
                    # Уже в базе - показываем через show_film_info_with_buttons
                    film_id = row.get('id') if isinstance(row, dict) else row[0]
                    cursor.execute("SELECT title, watched FROM movies WHERE id = %s", (film_id,))
                    movie_row = cursor.fetchone()
                    title = movie_row.get('title') if isinstance(movie_row, dict) else movie_row[0]
                    watched = movie_row.get('watched') if isinstance(movie_row, dict) else movie_row[1]
                    
                    show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=(film_id, title, watched))
                    return
            
            # НЕ в базе - показываем описание с ВСЕМИ кнопками БЕЗ добавления в базу
            show_film_info_without_adding(chat_id, user_id, info, link, kp_id)
            
        except Exception as e:
            logger.error(f"[KINOPOISK LINK] Ошибка обработки ссылки: {e}", exc_info=True)
            try:
                bot_instance.reply_to(message, "❌ Произошла ошибка при обработке ссылки.")
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("settings:"))
    def handle_settings_callback(call):
        """Обработчик callback для настроек"""
        # TODO: Извлечь полную реализацию из moviebot.py строки 21768-22476
        try:
            bot_instance.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            action = call.data.split(":", 1)[1]
            is_private = call.message.chat.type == 'private'
            
            logger.info(f"[SETTINGS CALLBACK] Получен callback от {user_id}, action={action}, chat_id={chat_id}, is_private={is_private}")
            
            if action == "random_events_locked":
                # Показываем сообщение о том, что раздел доступен только в групповых чатах
                try:
                    bot_instance.answer_callback_query(
                        call.id,
                        "Раздел доступен только в групповых чатах. Создайте групповой чат с друзьями, добавьте в него бота и планируйте просмотр кино вместе 👥",
                        show_alert=True
                    )
                except Exception as e:
                    logger.error(f"[SETTINGS] Ошибка при ответе на callback для random_events_locked: {e}")
                return
            
            if action == "random_events":
                # Проверяем, что это не личный чат
                if is_private:
                    bot_instance.answer_callback_query(
                        call.id,
                        "Раздел доступен только в групповых чатах. Создайте групповой чат с друзьями, добавьте в него бота и планируйте просмотр кино вместе 👥",
                        show_alert=True
                    )
                    return
                
                # TODO: Показать настройку случайных событий (извлечь из moviebot.py строки 21920-21963)
                logger.info(f"[SETTINGS] Показ настроек случайных событий для chat_id={chat_id}")
                bot_instance.answer_callback_query(call.id, "Настройки случайных событий будут реализованы позже")
                return
            
            # TODO: Добавить обработку остальных действий:
            # - settings:notifications
            # - settings:notifications_locked
            # - settings:import
            # - settings:import_locked
            # - settings:emoji
            # - settings:timezone
            # - settings:edit
            # - settings:clean
            # - settings:join
            # - settings:back
            # и другие из moviebot.py строки 21768-22476
            
            logger.warning(f"[SETTINGS CALLBACK] Необработанное действие: {action}")
        except Exception as e:
            logger.error(f"[SETTINGS CALLBACK] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    # TODO: Добавить остальные callback handlers:
    # - search_type callback
    # - search_back callback
    # - add_film callbacks
    # - random callbacks
    # - premieres callbacks
    # - ticket callbacks
    # и другие из moviebot.py


def show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=None, message_id=None, message_thread_id=None):
    """Показывает описание фильма с кнопками действий
    
    Args:
        chat_id: ID чата
        user_id: ID пользователя
        info: Информация о фильме из API
        link: Ссылка на Кинопоиск
        kp_id: ID фильма на Кинопоиске
        existing: Кортеж (film_id, title, watched) или None
        message_id: ID сообщения для обновления (если None - отправляет новое)
        message_thread_id: ID треда для групповых чатов
    """
    logger.info(f"[SHOW FILM INFO] ===== START: chat_id={chat_id}, user_id={user_id}, kp_id={kp_id}, message_id={message_id}")
    try:
        is_series = info.get('is_series', False)
        type_emoji = "📺" if is_series else "🎬"
        
        # Формируем текст описания
        text = f"{type_emoji} <b>{info['title']}</b> ({info['year'] or '—'})\n"
        if info.get('director'):
            text += f"<i>Режиссёр:</i> {info['director']}\n"
        if info.get('genres'):
            text += f"<i>Жанры:</i> {info['genres']}\n"
        if info.get('actors'):
            text += f"<i>В ролях:</i> {info['actors']}\n"
        if info.get('description'):
            text += f"\n<i>Кратко:</i> {info['description']}\n"
        
        # Если это сериал, добавляем информацию о статусе выхода серий
        if is_series:
            is_airing, next_episode = get_series_airing_status(kp_id)
            if is_airing and next_episode:
                text += f"\n🟢 <b>Сериал выходит сейчас</b>\n"
                text += f"📅 Следующая серия: Сезон {next_episode['season']}, Эпизод {next_episode['episode']} — {next_episode['date'].strftime('%d.%m.%Y')}\n"
            else:
                text += f"\n🔴 <b>Сериал не выходит</b>\n"
        
        text += f"\n<a href='{link}'>Кинопоиск</a>"
        
        # Если фильм уже в базе, показываем дополнительную информацию
        if existing:
            film_id = existing.get('id') if isinstance(existing, dict) else existing[0]
            watched = existing.get('watched') if isinstance(existing, dict) else existing[2]
            
            if watched:
                with db_lock:
                    cursor.execute('SELECT AVG(rating) as avg FROM ratings WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, film_id))
                    avg_result = cursor.fetchone()
                    if avg_result:
                        avg = avg_result.get('avg') if isinstance(avg_result, dict) else avg_result[0]
                        avg = float(avg) if avg is not None else None
                    else:
                        avg = None
                    
                    # Получаем личную оценку пользователя (если есть)
                    user_rating = None
                    if user_id:
                        cursor.execute('SELECT rating FROM ratings WHERE chat_id = %s AND film_id = %s AND user_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, film_id, user_id))
                        user_rating_row = cursor.fetchone()
                        if user_rating_row:
                            user_rating = user_rating_row.get('rating') if isinstance(user_rating_row, dict) else user_rating_row[0]
                
                text += f"\n\n✅ <b>Просмотрено</b>"
                if avg:
                    text += f"\n⭐ <b>Средняя оценка: {avg:.1f}/10</b>"
                # Добавляем строку о личной оценке пользователя (чтобы текст всегда менялся при обновлении)
                if user_rating is not None:
                    text += f"\n⭐ <b>Ваша оценка: {user_rating}/10</b>"
                else:
                    text += f"\n⭐ <b>Ваша оценка: —</b>"
            else:
                text += f"\n\n⏳ <b>Ещё не просмотрено</b>"
                # Добавляем строку о личной оценке пользователя даже если фильм не просмотрен (чтобы текст всегда менялся)
                if user_id and film_id:
                    with db_lock:
                        cursor.execute('SELECT rating FROM ratings WHERE chat_id = %s AND film_id = %s AND user_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, film_id, user_id))
                        user_rating_row = cursor.fetchone()
                        if user_rating_row:
                            user_rating = user_rating_row.get('rating') if isinstance(user_rating_row, dict) else user_rating_row[0]
                            if user_rating is not None:
                                text += f"\n⭐ <b>Ваша оценка: {user_rating}/10</b>"
                            else:
                                text += f"\n⭐ <b>Ваша оценка: —</b>"
                        else:
                            text += f"\n⭐ <b>Ваша оценка: —</b>"
        
        # Создаем кнопки
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Проверяем премьеру
        russia_release = info.get('russia_release')
        premiere_date = None
        premiere_date_str = ""
        
        if russia_release and russia_release.get('date'):
            premiere_date = russia_release['date']
            premiere_date_str = russia_release.get('date_str', premiere_date.strftime('%d.%m.%Y'))
        else:
            try:
                headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
                url_main = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}"
                response_main = requests.get(url_main, headers=headers, timeout=15)
                if response_main.status_code == 200:
                    data_main = response_main.json()
                    from datetime import date as date_class
                    today = date_class.today()
                    
                    for date_field in ['premiereWorld', 'premiereRu', 'premiereWorldDate', 'premiereRuDate']:
                        date_value = data_main.get(date_field)
                        if date_value:
                            try:
                                if 'T' in str(date_value):
                                    premiere_date = datetime.strptime(str(date_value).split('T')[0], '%Y-%m-%d').date()
                                else:
                                    premiere_date = datetime.strptime(str(date_value), '%Y-%m-%d').date()
                                premiere_date_str = premiere_date.strftime('%d.%m.%Y')
                                break
                            except:
                                continue
            except Exception as e:
                logger.warning(f"[SHOW FILM INFO] Ошибка получения информации о премьере: {e}")
        
        # Если премьера еще не состоялась, добавляем кнопку
        if premiere_date:
            from datetime import date as date_class
            today = date_class.today()
            if premiere_date > today:
                date_for_callback = premiere_date_str.replace(':', '-') if premiere_date_str else ''
                markup.add(InlineKeyboardButton("🔔 Уведомить о премьере", callback_data=f"premiere_notify:{kp_id}:{date_for_callback}:current_month"))
        
        # Добавляем основные кнопки
        markup.add(InlineKeyboardButton("📅 Запланировать просмотр", callback_data=f"plan_from_added:{kp_id}"))
        
        # Получаем film_id для проверки оценок
        film_id = None
        if existing:
            film_id = existing.get('id') if isinstance(existing, dict) else existing[0]
        else:
            with db_lock:
                cursor.execute("SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, kp_id))
                film_row = cursor.fetchone()
                if film_row:
                    film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
        
        if film_id:
            # Получаем информацию об оценках
            with db_lock:
                # Получаем среднюю оценку
                cursor.execute('''
                    SELECT AVG(rating) as avg FROM ratings 
                    WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
                ''', (chat_id, film_id))
                avg_result = cursor.fetchone()
                avg_rating = None
                if avg_result:
                    avg = avg_result.get('avg') if isinstance(avg_result, dict) else avg_result[0]
                    avg_rating = float(avg) if avg is not None else None
                
                # Получаем активных пользователей
                cursor.execute('''
                    SELECT DISTINCT user_id
                    FROM stats
                    WHERE chat_id = %s AND user_id IS NOT NULL
                ''', (chat_id,))
                active_users = {row.get('user_id') if isinstance(row, dict) else row[0] for row in cursor.fetchall()}
                
                # Получаем всех, кто оценил этот фильм
                cursor.execute('''
                    SELECT DISTINCT user_id FROM ratings
                    WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
                ''', (chat_id, film_id))
                rated_users = {row.get('user_id') if isinstance(row, dict) else row[0] for row in cursor.fetchall()}
                
                # Определяем текст и эмодзи кнопки
                if active_users and active_users.issubset(rated_users) and avg_rating is not None:
                    # Все активные пользователи оценили - показываем среднюю оценку
                    rating_int = int(round(avg_rating))
                    if 1 <= rating_int <= 4:
                        emoji = "💩"
                    elif 5 <= rating_int <= 7:
                        emoji = "💬"
                    else:  # 8-10
                        emoji = "🏆"
                    rating_text = f"{emoji} {avg_rating:.0f}/10"
                else:
                    rating_text = "💬 Оценить"
            
            markup.row(
                InlineKeyboardButton("🤔 Интересные факты", callback_data=f"show_facts:{kp_id}"),
                InlineKeyboardButton(rating_text, callback_data=f"rate_film:{kp_id}")
            )
            
            # Если это сериал, добавляем кнопки для сериалов
            if is_series and user_id:
                if has_notifications_access(chat_id, user_id):
                    # Проверяем, все ли серии просмотрены
                    seasons_data = get_seasons_data(kp_id)
                    all_episodes_watched = False
                    if seasons_data and film_id:
                        # Проверяем, выходит ли сериал
                        is_airing, _ = get_series_airing_status(kp_id)
                        
                        # Получаем просмотренные эпизоды
                        with db_lock:
                            cursor.execute('''
                                SELECT season_number, episode_number 
                                FROM series_tracking 
                                WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                            ''', (chat_id, film_id, user_id))
                            watched_rows = cursor.fetchall()
                            watched_set = set()
                            for w_row in watched_rows:
                                if isinstance(w_row, dict):
                                    watched_set.add((str(w_row.get('season_number')), str(w_row.get('episode_number'))))
                                else:
                                    watched_set.add((str(w_row[0]), str(w_row[1])))
                        
                        # Подсчитываем эпизоды
                        total_episodes, watched_episodes = count_episodes_for_watch_check(
                            seasons_data, is_airing, watched_set, chat_id, film_id, user_id
                        )
                        
                        if total_episodes > 0 and watched_episodes == total_episodes:
                            all_episodes_watched = True
                            # Отмечаем сериал как просмотренный в БД
                            with db_lock:
                                cursor.execute("UPDATE movies SET watched = 1 WHERE id = %s AND chat_id = %s", (film_id, chat_id))
                                conn.commit()
                    
                    # Проверяем подписку
                    is_subscribed = False
                    if film_id:
                        with db_lock:
                            cursor.execute('SELECT subscribed FROM series_subscriptions WHERE chat_id = %s AND film_id = %s AND user_id = %s', (chat_id, film_id, user_id))
                            sub_row = cursor.fetchone()
                            is_subscribed = sub_row and (sub_row.get('subscribed') if isinstance(sub_row, dict) else sub_row[0])
                    
                    # Добавляем строку о статусе подписки в текст (чтобы текст всегда менялся)
                    if is_subscribed:
                        text += f"\n\n🔔 <b>Статус подписки: ✅ Подписан</b>"
                    else:
                        text += f"\n\n🔔 <b>Статус подписки: ❌ Не подписан</b>"
                    
                    # Показываем соответствующую кнопку
                    if all_episodes_watched:
                        markup.add(InlineKeyboardButton("✅ Просмотрено", callback_data=f"series_track:{kp_id}"))
                    else:
                        markup.add(InlineKeyboardButton("✅ Отметить просмотренные серии", callback_data=f"series_track:{kp_id}"))
                    
                    # Кнопка подписки
                    if is_subscribed:
                        markup.add(InlineKeyboardButton("🔕 Убрать подписку на новые серии", callback_data=f"series_unsubscribe:{kp_id}"))
                    else:
                        markup.add(InlineKeyboardButton("🔔 Подписаться на новые серии", callback_data=f"series_subscribe:{kp_id}"))
                else:
                    markup.add(InlineKeyboardButton("🔒 Отметить просмотренные серии", callback_data=f"series_locked:{kp_id}"))
                    markup.add(InlineKeyboardButton("🔒 Подписаться на новые серии", callback_data=f"series_locked:{kp_id}"))
        
        # Проверяем, есть ли план для этого фильма (дома)
        if film_id:
            with db_lock:
                cursor.execute('''
                    SELECT id, plan_type FROM plans 
                    WHERE film_id = %s AND chat_id = %s
                    ORDER BY plan_datetime ASC
                    LIMIT 1
                ''', (film_id, chat_id))
                plan_row = cursor.fetchone()
            
            if plan_row:
                plan_id = plan_row.get('id') if isinstance(plan_row, dict) else plan_row[0]
                plan_type = plan_row.get('plan_type') if isinstance(plan_row, dict) else plan_row[1]
                
                # Добавляем кнопки только для планов "дома"
                if plan_type == 'home':
                    # Кнопка "Отметить просмотренным" (если фильм еще не просмотрен)
                    if existing:
                        watched = existing.get('watched') if isinstance(existing, dict) else existing[2]
                        if not watched:
                            markup.add(InlineKeyboardButton("✅ Отметить просмотренным", callback_data=f"mark_watched_from_description:{film_id}"))
                    
                    # Кнопки "Изменить" и "Удалить" в одной строке
                    markup.row(
                        InlineKeyboardButton("✏️ Изменить", callback_data=f"edit_plan:{plan_id}"),
                        InlineKeyboardButton("🗑️ Удалить", callback_data=f"remove_from_calendar:{plan_id}")
                    )
        
        # Отправляем или обновляем сообщение
        if message_id:
            # Обновляем существующее сообщение
            try:
                if message_thread_id:
                    # Для тредов используем API напрямую
                    import json
                    reply_markup_json = json.dumps(markup.to_dict()) if markup else None
                    params = {
                        'chat_id': chat_id,
                        'message_id': message_id,
                        'text': text,
                        'parse_mode': 'HTML',
                        'disable_web_page_preview': False,
                        'message_thread_id': message_thread_id
                    }
                    if reply_markup_json:
                        params['reply_markup'] = reply_markup_json
                    bot_instance.api_call('editMessageText', params)
                else:
                    bot_instance.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=False)
                logger.info(f"[SHOW FILM INFO] Сообщение обновлено: {info.get('title')}, kp_id={kp_id}, message_id={message_id}")
            except telebot.apihelper.ApiTelegramException as e:
                error_str = str(e).lower()
                logger.error(f"[SHOW FILM INFO] Telegram API ошибка при обновлении сообщения: {e}", exc_info=True)
                logger.error(f"[SHOW FILM INFO] error_code={getattr(e, 'error_code', 'N/A')}, result_json={getattr(e, 'result_json', {})}")
                
                # Проверяем, является ли это ошибкой "message is not modified"
                if "message is not modified" in error_str or "message_not_modified" in error_str or "bad request: message is not modified" in error_str:
                    # Если текст не изменился — просто обновляем клавиатуру
                    logger.info(f"[SHOW FILM INFO] Текст не изменился, обновляю только клавиатуру...")
                    try:
                        if message_thread_id:
                            import json
                            reply_markup_json = json.dumps(markup.to_dict()) if markup else None
                            params = {
                                'chat_id': chat_id,
                                'message_id': message_id,
                                'message_thread_id': message_thread_id
                            }
                            if reply_markup_json:
                                params['reply_markup'] = reply_markup_json
                            bot_instance.api_call('editMessageReplyMarkup', params)
                        else:
                            bot_instance.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=markup)
                        logger.info(f"[SHOW FILM INFO] Клавиатура обновлена успешно")
                    except Exception as e2:
                        logger.error(f"[SHOW FILM INFO] Не удалось обновить markup: {e2}", exc_info=True)
                        # При ошибке отправляем новое сообщение
                        try:
                            if message_thread_id:
                                bot_instance.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup, message_thread_id=message_thread_id)
                            else:
                                bot_instance.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup)
                            logger.info(f"[SHOW FILM INFO] Отправлено новое сообщение вместо обновления: {info.get('title')}, kp_id={kp_id}")
                        except Exception as send_e:
                            logger.error(f"[SHOW FILM INFO] Не удалось отправить новое сообщение: {send_e}", exc_info=True)
                else:
                    # Другая ошибка API - отправляем новое сообщение
                    logger.warning(f"[SHOW FILM INFO] Другая ошибка Telegram API, отправляю новое сообщение")
                    try:
                        if message_thread_id:
                            bot_instance.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup, message_thread_id=message_thread_id)
                        else:
                            bot_instance.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup)
                        logger.info(f"[SHOW FILM INFO] Отправлено новое сообщение вместо обновления: {info.get('title')}, kp_id={kp_id}")
                    except Exception as send_e:
                        logger.error(f"[SHOW FILM INFO] Не удалось отправить новое сообщение: {send_e}", exc_info=True)
            except Exception as e:
                logger.error(f"[SHOW FILM INFO] Неизвестная ошибка обновления сообщения: {e}", exc_info=True)
                # При ошибке отправляем новое сообщение
                try:
                    if message_thread_id:
                        bot_instance.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup, message_thread_id=message_thread_id)
                    else:
                        bot_instance.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup)
                    logger.info(f"[SHOW FILM INFO] Отправлено новое сообщение вместо обновления: {info.get('title')}, kp_id={kp_id}")
                except Exception as send_e:
                    logger.error(f"[SHOW FILM INFO] Не удалось отправить новое сообщение: {send_e}", exc_info=True)
        else:
            # Отправляем новое сообщение
            try:
                if message_thread_id:
                    bot_instance.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup, message_thread_id=message_thread_id)
                else:
                    bot_instance.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup)
                logger.info(f"[SHOW FILM INFO] Описание фильма отправлено: {info.get('title')}, kp_id={kp_id}")
            except Exception as send_e:
                logger.error(f"[SHOW FILM INFO] Не удалось отправить новое сообщение: {send_e}", exc_info=True)
        
    except Exception as e:
        logger.error(f"[SHOW FILM INFO] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.send_message(chat_id, "❌ Произошла ошибка при показе описания фильма.")
        except:
            pass


def ensure_movie_in_database(chat_id, kp_id, link, info, user_id=None):
    """
    Добавляет фильм/сериал в базу, если его еще нет.
    Возвращает (film_id, was_inserted), где was_inserted = True если фильм был добавлен.
    """
    try:
        with db_lock:
            # Проверяем, существует ли фильм
            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
            
            if row:
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                logger.info(f"[ENSURE MOVIE] Фильм уже в базе: film_id={film_id}, kp_id={kp_id}")
                return film_id, False
            
            # Добавляем фильм в базу
            cursor.execute('''
                INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series, added_by, added_at, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 'link')
                ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link, is_series = EXCLUDED.is_series
                RETURNING id
            ''', (chat_id, link, kp_id, info['title'], info['year'], info['genres'], info['description'], 
                  info['director'], info['actors'], 1 if info.get('is_series') else 0, user_id))
            
            result = cursor.fetchone()
            film_id = result.get('id') if isinstance(result, dict) else result[0]
            conn.commit()
            
            logger.info(f"[ENSURE MOVIE] Фильм добавлен в базу: film_id={film_id}, kp_id={kp_id}, title={info['title']}")
            return film_id, True
            
    except Exception as e:
        logger.error(f"[ENSURE MOVIE] Ошибка при добавлении фильма в базу: {e}", exc_info=True)
        conn.rollback()
        return None, False


def show_film_info_without_adding(chat_id, user_id, info, link, kp_id):
    """
    Показывает описание фильма/сериала с ВСЕМИ кнопками БЕЗ добавления в базу.
    Используется когда пользователь отправляет ссылку на сериал.
    """
    try:
        is_series = info.get('is_series', False)
        type_emoji = "📺" if is_series else "🎬"
        
        # Формируем текст описания
        text = f"{type_emoji} <b>{info['title']}</b> ({info['year'] or '—'})\n"
        if info.get('director'):
            text += f"<i>Режиссёр:</i> {info['director']}\n"
        if info.get('genres'):
            text += f"<i>Жанры:</i> {info['genres']}\n"
        if info.get('actors'):
            text += f"<i>В ролях:</i> {info['actors']}\n"
        if info.get('description'):
            text += f"\n<i>Кратко:</i> {info['description']}\n"
        
        # Если это сериал, добавляем информацию о статусе выхода серий
        if is_series:
            is_airing, next_episode = get_series_airing_status(kp_id)
            if is_airing and next_episode:
                text += f"\n🟢 <b>Сериал выходит сейчас</b>\n"
                text += f"📅 Следующая серия: Сезон {next_episode['season']}, Эпизод {next_episode['episode']} — {next_episode['date'].strftime('%d.%m.%Y')}\n"
            else:
                text += f"\n🔴 <b>Сериал не выходит</b>\n"
        
        text += f"\n<a href='{link}'>Кинопоиск</a>"
        text += f"\n\n⏳ <b>Ещё не просмотрено</b>"
        
        # Создаем кнопки
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Проверяем премьеру
        russia_release = info.get('russia_release')
        premiere_date = None
        premiere_date_str = ""
        
        if russia_release and russia_release.get('date'):
            premiere_date = russia_release['date']
            premiere_date_str = russia_release.get('date_str', premiere_date.strftime('%d.%m.%Y'))
        else:
            try:
                headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
                url_main = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}"
                response_main = requests.get(url_main, headers=headers, timeout=15)
                if response_main.status_code == 200:
                    data_main = response_main.json()
                    from datetime import date as date_class
                    today = date_class.today()
                    
                    for date_field in ['premiereWorld', 'premiereRu', 'premiereWorldDate', 'premiereRuDate']:
                        date_value = data_main.get(date_field)
                        if date_value:
                            try:
                                if 'T' in str(date_value):
                                    premiere_date = datetime.strptime(str(date_value).split('T')[0], '%Y-%m-%d').date()
                                else:
                                    premiere_date = datetime.strptime(str(date_value), '%Y-%m-%d').date()
                                premiere_date_str = premiere_date.strftime('%d.%m.%Y')
                                break
                            except:
                                continue
            except Exception as e:
                logger.warning(f"[SHOW FILM INFO] Ошибка получения информации о премьере: {e}")
        
        # Если премьера еще не состоялась, добавляем кнопку
        if premiere_date:
            from datetime import date as date_class
            today = date_class.today()
            if premiere_date > today:
                date_for_callback = premiere_date_str.replace(':', '-') if premiere_date_str else ''
                markup.add(InlineKeyboardButton("🔔 Уведомить о премьере", callback_data=f"premiere_notify:{kp_id}:{date_for_callback}:current_month"))
        
        # Добавляем основные кнопки
        markup.add(InlineKeyboardButton("📅 Запланировать просмотр", callback_data=f"plan_from_added:{kp_id}"))
        
        # Добавляем кнопки для всех действий (сериал будет добавлен в базу при нажатии)
        markup.row(
            InlineKeyboardButton("🤔 Интересные факты", callback_data=f"show_facts:{kp_id}"),
            InlineKeyboardButton("💬 Оценить", callback_data=f"rate_film:{kp_id}")
        )
        
        # Если это сериал, добавляем кнопки для сериалов
        if is_series:
            if user_id and has_notifications_access(chat_id, user_id):
                markup.add(InlineKeyboardButton("✅ Отметить просмотренные серии", callback_data=f"series_track:{kp_id}"))
                markup.add(InlineKeyboardButton("🔔 Подписаться на новые серии", callback_data=f"series_subscribe:{kp_id}"))
            else:
                markup.add(InlineKeyboardButton("🔒 Отметить просмотренные серии", callback_data=f"series_locked:{kp_id}"))
                markup.add(InlineKeyboardButton("🔒 Подписаться на новые серии", callback_data=f"series_locked:{kp_id}"))
        
        # Отправляем сообщение
        msg = bot_instance.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup)
        logger.info(f"[SHOW FILM INFO] Описание фильма отправлено БЕЗ добавления в базу: {info.get('title')}, kp_id={kp_id}")
        return msg
        
    except Exception as e:
        logger.error(f"[SHOW FILM INFO] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.send_message(chat_id, "❌ Произошла ошибка при показе описания фильма.")
        except:
            pass
        return None
