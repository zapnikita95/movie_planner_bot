"""
API модуль для работы с Kinopoisk API
"""
import re
import requests
import logging
from datetime import datetime
from config.settings import KP_TOKEN
from database.db_connection import get_db_connection, get_db_cursor, db_lock

# Получаем глобальные объекты БД
conn = get_db_connection()
cursor = get_db_cursor()

logger = logging.getLogger(__name__)

def extract_movie_info(link):
    match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', link)
    if not match:
        logger.warning(f"Не распознана ссылка: {link}")
        return None
    kp_id = match.group(2)
    is_series = match.group(1) == 'series'  # Определяем, сериал это или фильм

    headers = {
        'X-API-KEY': KP_TOKEN,
        'Content-Type': 'application/json'
    }

    try:
        # Основные данные (название, год, жанры, описание)
        url_main = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}"
        response_main = requests.get(url_main, headers=headers, timeout=15)
        if response_main.status_code != 200:
            logger.error(f"Основной запрос ошибка {response_main.status_code}")
            return None
        data_main = response_main.json()

        title = data_main.get('nameRu') or data_main.get('nameOriginal') or "Unknown"
        year = data_main.get('year') or "—"
        genres = ', '.join([g['genre'] for g in data_main.get('genres', [])]) or "—"
        description = data_main.get('description') or data_main.get('shortDescription') or "Нет описания"

        # Отдельный запрос на staff (режиссёр и актёры)
        # Используем v1 endpoint как основной, так как v2.2 не работает
        url_staff = f"https://kinopoiskapiunofficial.tech/api/v1/staff?filmId={kp_id}"
        logger.debug(f"Staff запрос URL: {url_staff}")
        response_staff = requests.get(url_staff, headers=headers, timeout=15)
        staff = []
        if response_staff.status_code == 200:
            staff = response_staff.json()
            logger.debug(f"Staff ответ получен, количество записей: {len(staff) if isinstance(staff, list) else 'не список'}")
        else:
            logger.warning(f"Staff запрос ошибка {response_staff.status_code} — режиссёр/актёры не загружены")
            logger.warning(f"Staff ответ: {response_staff.text[:200] if response_staff.text else 'нет текста'}")

        # Режиссёр
        director = "Не указан"
        if staff and len(staff) > 0:
            # Логируем структуру первого элемента для отладки
            logger.debug(f"Пример структуры staff элемента: {list(staff[0].keys()) if isinstance(staff[0], dict) else 'не словарь'}")
        
        for person in staff:
            if not isinstance(person, dict):
                continue
            # Проверяем разные варианты полей для профессии
            profession = person.get('professionKey') or person.get('professionText') or person.get('profession')
            if profession and ('DIRECTOR' in str(profession).upper() or 'РЕЖИССЕР' in str(profession).upper() or profession == 'DIRECTOR'):
                # Проверяем разные варианты полей для имени
                name = person.get('nameRu') or person.get('nameEn') or person.get('name') or person.get('staffName')
                if name:
                    director = name
                    break

        # Актёры (top 6)
        actors_list = []
        for person in staff:
            if not isinstance(person, dict):
                continue
            # Проверяем разные варианты полей для профессии
            profession = person.get('professionKey') or person.get('professionText') or person.get('profession')
            if profession and ('ACTOR' in str(profession).upper() or 'АКТЕР' in str(profession).upper() or profession == 'ACTOR') and len(actors_list) < 6:
                # Проверяем разные варианты полей для имени
                name = person.get('nameRu') or person.get('nameEn') or person.get('name') or person.get('staffName')
                if name:
                    actors_list.append(name)
        actors = ', '.join(actors_list) if actors_list else "—"

        logger.info(f"Успешно: {title} ({year}), режиссёр: {director}, актёры: {actors}")

        return {
            'kp_id': kp_id,
            'title': title,
            'year': year,
            'genres': genres,
            'director': director,
            'actors': actors,
            'description': description,
            'is_series': is_series
        }
    except Exception as e:
        logger.error(f"Ошибка получения данных для {kp_id}: {e}")
        return None


def get_facts(kp_id):
    """Получает интересные факты о фильме"""
    headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/facts"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            facts = data.get('items', [])
            if facts:
                # Разделяем факты на Факты и Ошибки
                facts_list = []
                bloopers_list = []
                
                for fact in facts:
                    fact_text = fact.get('text', '').strip()
                    fact_type = fact.get('type', '')
                    if fact_text:
                        # Исправляем HTML-сущности
                        fact_text = fact_text.replace('&laquo;', '«').replace('&raquo;', '»').replace('&quot;', '"').replace('&amp;', '&')
                        if fact_type == 'FACT':
                            facts_list.append((fact_type, fact_text))
                        elif fact_type == 'BLOOPER':
                            bloopers_list.append((fact_type, fact_text))
                
                text = "🤔 <b>Интересные факты о фильме:</b>\n\n"
                
                # Сначала Факты
                if facts_list:
                    for fact_type, fact_text in facts_list[:3]:  # Максимум 3 факта
                        text += f"• <b>Факты:</b> {fact_text}\n\n"
                
                # Потом Ошибки
                if bloopers_list:
                    for fact_type, fact_text in bloopers_list[:3]:  # Максимум 3 блупера
                        text += f"• <b>Ошибки:</b> {fact_text}\n\n"
                
                return text if (facts_list or bloopers_list) else None
            else:
                return None
        else:
            logger.error(f"Ошибка get_facts: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Ошибка get_facts: {e}", exc_info=True)
        return None


def get_seasons(kp_id, chat_id=None, user_id=None):
    """Получает информацию о сезонах сериала с отметками просмотренных"""
    headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
    # Пробуем сначала v2.2, если не работает - v2.1
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/seasons"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            seasons = data.get('items', [])
            if seasons:
                # Получаем информацию о просмотренных сериях
                watched_episodes = set()
                if chat_id and user_id:
                    with db_lock:
                        cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                        row = cursor.fetchone()
                        if row:
                            film_id = row.get('id') if isinstance(row, dict) else row[0]
                            cursor.execute('''
                                SELECT season_number, episode_number 
                                FROM series_tracking 
                                WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                            ''', (chat_id, film_id, user_id))
                            watched_rows = cursor.fetchall()
                            for w_row in watched_rows:
                                if isinstance(w_row, dict):
                                    watched_episodes.add((w_row.get('season_number'), w_row.get('episode_number')))
                                else:
                                    watched_episodes.add((w_row[0], w_row[1]))
                
                from datetime import datetime as dt
                now = dt.now()
                
                # Получаем информацию о выходе серий
                next_episode = None
                next_episode_date = None
                is_airing = False
                
                for season in seasons:
                    episodes = season.get('episodes', [])
                    for ep in episodes:
                        release_str = ep.get('releaseDate', '')
                        if release_str and release_str != '—':
                            try:
                                # Пробуем разные форматы даты
                                release_date = None
                                for fmt in ['%Y-%m-%d', '%d.%m.%Y', '%Y-%m-%dT%H:%M:%S']:
                                    try:
                                        release_date = dt.strptime(release_str.split('T')[0], fmt)
                                        break
                                    except:
                                        continue
                                
                                if release_date and release_date > now:
                                    if not next_episode_date or release_date < next_episode_date:
                                        next_episode_date = release_date
                                        next_episode = {
                                            'season': season.get('number', ''),
                                            'episode': ep.get('episodeNumber', ''),
                                            'date': release_date
                                        }
                                        is_airing = True
                            except:
                                pass
                
                # Подсчитываем просмотренные сезоны
                season_stats = {}
                for season in seasons:
                    number = season.get('number', '')
                    episodes = season.get('episodes', [])
                    watched_in_season = sum(1 for ep in episodes if (number, str(ep.get('episodeNumber', ''))) in watched_episodes)
                    total_in_season = len(episodes)
                    season_stats[number] = {'watched': watched_in_season, 'total': total_in_season}
                
                text = "📺 <b>Сезоны сериала:</b>\n\n"
                for season in seasons:
                    number = season.get('number', '')
                    episodes = season.get('episodes', [])
                    stats = season_stats.get(number, {'watched': 0, 'total': len(episodes)})
                    
                    # Определяем статус сезона
                    if stats['watched'] == stats['total'] and stats['total'] > 0:
                        status = "✅ Просмотрен полностью"
                    elif stats['watched'] > 0:
                        status = f"⏳ Просмотрено {stats['watched']}/{stats['total']}"
                    else:
                        status = "⬜ Не просмотрен"
                    
                    text += f"<b>Сезон {number}</b> ({stats['total']} серий) — {status}\n"
                
                text += "\n"
                
                # Информация о выходе серий
                if is_airing and next_episode:
                    text += f"🟢 <b>Сериал выходит сейчас</b>\n"
                    text += f"📅 Следующая серия: Сезон {next_episode['season']}, Эпизод {next_episode['episode']} — {next_episode['date'].strftime('%d.%m.%Y')}\n\n"
                else:
                    text += f"🔴 <b>Сериал не выходит</b>\n\n"
                
                return text
            else:
                return None
        elif response.status_code == 400:
            # Пробуем v2.1 если v2.2 не работает
            logger.warning(f"Ошибка 400 для v2.2, пробуем v2.1 для kp_id={kp_id}")
            url = f"https://kinopoiskapiunofficial.tech/api/v2.1/films/{kp_id}/seasons"
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                data = response.json()
                seasons = data.get('items', [])
                if seasons:
                    # Получаем информацию о просмотренных сериях
                    watched_episodes = set()
                    if chat_id and user_id:
                        with db_lock:
                            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                            row = cursor.fetchone()
                            if row:
                                film_id = row.get('id') if isinstance(row, dict) else row[0]
                                cursor.execute('''
                                    SELECT season_number, episode_number 
                                    FROM series_tracking 
                                    WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                                ''', (chat_id, film_id, user_id))
                                watched_rows = cursor.fetchall()
                                for w_row in watched_rows:
                                    if isinstance(w_row, dict):
                                        watched_episodes.add((w_row.get('season_number'), w_row.get('episode_number')))
                                    else:
                                        watched_episodes.add((w_row[0], w_row[1]))
                    
                    from datetime import datetime as dt
                    now = dt.now()
                    
                    # Получаем информацию о выходе серий
                    next_episode = None
                    next_episode_date = None
                    is_airing = False
                    
                    for season in seasons:
                        episodes = season.get('episodes', [])
                        for ep in episodes:
                            release_str = ep.get('releaseDate', '')
                            if release_str and release_str != '—':
                                try:
                                    # Пробуем разные форматы даты
                                    release_date = None
                                    for fmt in ['%Y-%m-%d', '%d.%m.%Y', '%Y-%m-%dT%H:%M:%S']:
                                        try:
                                            release_date = dt.strptime(release_str.split('T')[0], fmt)
                                            break
                                        except:
                                            continue
                                    
                                    if release_date and release_date > now:
                                        if not next_episode_date or release_date < next_episode_date:
                                            next_episode_date = release_date
                                            next_episode = {
                                                'season': season.get('number', ''),
                                                'episode': ep.get('episodeNumber', ''),
                                                'date': release_date
                                            }
                                            is_airing = True
                                except:
                                    pass
                    
                    # Подсчитываем просмотренные сезоны
                    season_stats = {}
                    for season in seasons:
                        number = season.get('number', '')
                        episodes = season.get('episodes', [])
                        watched_in_season = sum(1 for ep in episodes if (number, str(ep.get('episodeNumber', ''))) in watched_episodes)
                        total_in_season = len(episodes)
                        season_stats[number] = {'watched': watched_in_season, 'total': total_in_season}
                    
                    text = "📺 <b>Сезоны сериала:</b>\n\n"
                    for season in seasons:
                        number = season.get('number', '')
                        episodes = season.get('episodes', [])
                        stats = season_stats.get(number, {'watched': 0, 'total': len(episodes)})
                        
                        # Определяем статус сезона
                        if stats['watched'] == stats['total'] and stats['total'] > 0:
                            status = "✅ Просмотрен полностью"
                        elif stats['watched'] > 0:
                            status = f"⏳ Просмотрено {stats['watched']}/{stats['total']}"
                        else:
                            status = "⬜ Не просмотрен"
                        
                        text += f"<b>Сезон {number}</b> ({stats['total']} серий) — {status}\n"
                    
                    text += "\n"
                    
                    # Информация о выходе серий
                    if is_airing and next_episode:
                        text += f"🟢 <b>Сериал выходит сейчас</b>\n"
                        text += f"📅 Следующая серия: Сезон {next_episode['season']}, Эпизод {next_episode['episode']} — {next_episode['date'].strftime('%d.%m.%Y')}\n\n"
                    else:
                        text += f"🔴 <b>Сериал не выходит</b>\n\n"
                    
                    return text
                else:
                    return None
            else:
                logger.error(f"Ошибка get_seasons (v2.1): {response.status_code}, response: {response.text[:200]}")
                return None
        else:
            logger.error(f"Ошибка get_seasons: {response.status_code}, response: {response.text[:200]}")
            return None
    except Exception as e:
        logger.error(f"Ошибка get_seasons: {e}", exc_info=True)
        return None


def get_seasons_data(kp_id):
    """Получает данные о сезонах сериала (возвращает список сезонов)"""
    headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
    # Пробуем сначала v2.2, если не работает - v2.1
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/seasons"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            return data.get('items', [])
        elif response.status_code == 400:
            # Пробуем v2.1 если v2.2 не работает
            url = f"https://kinopoiskapiunofficial.tech/api/v2.1/films/{kp_id}/seasons"
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                data = response.json()
                return data.get('items', [])
            else:
                logger.error(f"Ошибка get_seasons_data (v2.1): {response.status_code}, response: {response.text[:200]}")
                return []
        else:
            logger.error(f"Ошибка get_seasons_data: {response.status_code}, response: {response.text[:200]}")
            return []
    except Exception as e:
        logger.error(f"Ошибка get_seasons_data: {e}", exc_info=True)
        return []


def get_similars(kp_id):
    """Получает похожие фильмы"""
    headers = {'X-API-KEY': KP_TOKEN}
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/similars"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            similars = data.get('items', [])
            return [(s.get('filmId'), s.get('nameRu') or s.get('nameEn', 'Без названия')) for s in similars[:5]]
        return []
    except Exception as e:
        logger.error(f"Ошибка get_similars: {e}", exc_info=True)
        return []


def get_sequels(kp_id):
    """Получает продолжения, приквелы и ремейки, разделяет по типам"""
    headers = {'X-API-KEY': KP_TOKEN}
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/sequels_and_prequels"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            
            sequels = []  # Сиквелы и приквелы
            remakes = []  # Ремейки
            
            for item in items:
                film_id = item.get('filmId')
                name = item.get('nameRu') or item.get('nameEn', 'Без названия')
                relation_type = item.get('relationType', '').upper()
                
                if film_id and name:
                    # Проверяем тип связи
                    if 'REMAKE' in relation_type or 'REMADE' in relation_type:
                        remakes.append((film_id, name))
                    else:
                        # Сиквелы, приквелы и другие связи
                        sequels.append((film_id, name))
            
            return {
                'sequels': sequels[:5],  # Максимум 5
                'remakes': remakes[:5]   # Максимум 5
            }
        return {'sequels': [], 'remakes': []}
    except Exception as e:
        logger.error(f"Ошибка get_sequels: {e}", exc_info=True)
        return {'sequels': [], 'remakes': []}


def get_external_sources(kp_id):
    """Получает внешние источники для просмотра фильма"""
    headers = {'X-API-KEY': KP_TOKEN}
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/external_sources"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            sources = data.get('items', [])
            links = []
            for s in sources:
                if s.get('url'):
                    platform = s.get('platform', 'Смотреть')
                    links.append((platform, s['url']))
            return links
        return []
    except Exception as e:
        logger.error(f"Ошибка get_external_sources: {e}", exc_info=True)
        return []


def get_premieres_for_period(period_type='current_month'):
    """Получает список премьер для указанного периода"""
    now = datetime.now()
    headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
    
    all_premieres = []
    
    if period_type == 'current_month':
        # Текущий месяц
        months = [(now.year, now.month)]
    elif period_type == 'next_month':
        # Следующий месяц
        next_month = now.month + 1
        next_year = now.year
        if next_month > 12:
            next_month = 1
            next_year += 1
        months = [(next_year, next_month)]
    elif period_type == '3_months':
        # 3 месяца
        months = []
        for i in range(3):
            month = now.month + i
            year = now.year
            while month > 12:
                month -= 12
                year += 1
            months.append((year, month))
    elif period_type == '6_months':
        # 6 месяцев
        months = []
        for i in range(6):
            month = now.month + i
            year = now.year
            while month > 12:
                month -= 12
                year += 1
            months.append((year, month))
    elif period_type == 'current_year':
        # Текущий год (до 31 декабря)
        months = [(now.year, m) for m in range(now.month, 13)]
    elif period_type == 'next_year':
        # Ближайший год (следующий год полностью)
        months = [(now.year + 1, m) for m in range(1, 13)]
    else:
        months = [(now.year, now.month)]
    
    # Получаем премьеры для каждого месяца
    # API требует месяц в формате JANUARY, FEBRUARY и т.д. для v2.2
    month_names = ['JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE',
                   'JULY', 'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER']
    
    for year, month in months:
        month_name = month_names[month - 1] if 1 <= month <= 12 else 'JANUARY'
        urls_to_try = [
            # v2.2 требует название месяца
            f"https://kinopoiskapiunofficial.tech/api/v2.2/films/premieres?year={year}&month={month_name}",
            # v2.1 может принимать число
            f"https://kinopoiskapiunofficial.tech/api/v2.1/films/premieres?year={year}&month={month}",
        ]
        
        for url in urls_to_try:
            try:
                logger.info(f"[PREMIERES] Запрос к API: {url}")
                response = requests.get(url, headers=headers, timeout=15)
                logger.info(f"[PREMIERES] Статус ответа: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    premieres = data.get('releases', []) or data.get('items', []) or data.get('premieres', [])
                    if premieres:
                        logger.info(f"[PREMIERES] Получено премьер для {year}-{month:02d}: {len(premieres)}")
                        all_premieres.extend(premieres)
                        break  # Успешно получили, переходим к следующему месяцу
                elif response.status_code != 400:
                    logger.warning(f"[PREMIERES] Ошибка {response.status_code} для {url}: {response.text[:200]}")
                    continue
                else:
                    logger.warning(f"[PREMIERES] Ошибка 400 для {url}: {response.text[:200]}")
                    continue
            except Exception as e:
                logger.warning(f"[PREMIERES] Ошибка при запросе {url}: {e}")
                continue
    
    # Убираем дубликаты по kinopoiskId
    seen_ids = set()
    unique_premieres = []
    for p in all_premieres:
        kp_id = p.get('kinopoiskId') or p.get('filmId')
        if kp_id and kp_id not in seen_ids:
            seen_ids.add(kp_id)
            unique_premieres.append(p)
    
    logger.info(f"[PREMIERES] Всего уникальных премьер: {len(unique_premieres)}")
    return unique_premieres


def get_premieres(year=None, month=None):
    """Получает список премьер на указанный месяц (старая функция для обратной совместимости)"""
    if not year:
        year = datetime.now().year
    if not month:
        month = datetime.now().month
    
    headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/premieres?year={year}&month={month}"
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            premieres = data.get('releases', []) or data.get('items', []) or data.get('premieres', [])
            return premieres
    except Exception as e:
        logger.error(f"[PREMIERES] Ошибка: {e}")
    
        return []

# Новая функция для поиска фильмов через API

def search_films(query, page=1):
    """Поиск фильмов через Kinopoisk API"""
    if not KP_TOKEN:
        logger.error("[SEARCH] KP_TOKEN не установлен")
        return [], 0
    
    # Используем правильный endpoint для поиска
    url = f"https://kinopoiskapiunofficial.tech/api/v2.1/films/search-by-keyword"
    params = {"keyword": query, "page": page}
    headers = {
        "X-API-KEY": KP_TOKEN,
        "accept": "application/json"
    }
    
    logger.info(f"[SEARCH] Запрос: query='{query}', page={page}, url={url}")
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=15)
        logger.info(f"[SEARCH] Статус ответа: {response.status_code}")
        logger.info(f"[SEARCH] URL запроса: {response.url}")
        
        if response.status_code != 200:
            logger.error(f"[SEARCH] Ошибка API: статус {response.status_code}, ответ: {response.text[:500]}")
            return [], 0
        
        data = response.json()
        items = data.get("films", []) or data.get("items", [])
        total_pages = data.get("totalPages", 1) or data.get("pagesCount", 1)
        logger.info(f"[SEARCH] Найдено результатов: {len(items)}, всего страниц: {total_pages}")
        
        # Логируем структуру первого элемента для отладки
        if items and len(items) > 0:
            first_item = items[0]
            logger.info(f"[SEARCH] Структура первого элемента: {list(first_item.keys()) if isinstance(first_item, dict) else 'не словарь'}")
            logger.info(f"[SEARCH] Пример данных: nameRu={first_item.get('nameRu')}, nameEn={first_item.get('nameEn')}, kinopoiskId={first_item.get('kinopoiskId')}, filmId={first_item.get('filmId')}")
        
        return items, total_pages
    except requests.exceptions.RequestException as e:
        logger.error(f"[SEARCH] Ошибка запроса: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"[SEARCH] Ответ сервера: {e.response.text[:500]}")
        return [], 0
    except Exception as e:
        logger.error(f"[SEARCH] Неожиданная ошибка: {e}", exc_info=True)
        return [], 0

# Добавление и анонс

