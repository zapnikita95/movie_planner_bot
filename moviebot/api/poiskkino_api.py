"""
API модуль для работы с ПоискКино API (poiskkino.dev)
Резервный API для kinopoiskapiunofficial.tech
"""
import re
import requests
import logging
from datetime import datetime, date
from moviebot.config import POISKKINO_TOKEN
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock

# Получаем глобальные объекты БД
conn = get_db_connection()
cursor = get_db_cursor()

logger = logging.getLogger(__name__)

BASE_URL = "https://api.poiskkino.dev"


def log_poiskkino_api_request(endpoint, method='GET', status_code=None, user_id=None, chat_id=None, kp_id=None):
    """Логирует запрос к API ПоискКино в БД"""
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    with db_lock:
        try:
            try:
                conn_local.rollback()
            except:
                pass

            kp_id_str = str(kp_id) if kp_id is not None else None

            cursor_local.execute('''
                INSERT INTO kinopoisk_api_logs 
                (endpoint, method, status_code, user_id, chat_id, kp_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (f"[POISKKINO]{endpoint}", method, status_code, user_id, chat_id, kp_id_str))
            
            conn_local.commit()
            
        except Exception as e:
            logger.error(f"Ошибка логирования PoisKino API-запроса: {e}", exc_info=True)
            try:
                conn_local.rollback()
            except:
                pass


def _get_headers():
    """Возвращает заголовки для запросов к API"""
    return {
        'X-API-KEY': POISKKINO_TOKEN,
        'Content-Type': 'application/json',
        'accept': 'application/json'
    }


def _map_type_to_is_series(movie_type):
    """Преобразует тип из poiskkino в is_series"""
    if movie_type in ['tv-series', 'animated-series', 'anime']:
        return True
    return False


def _map_type_from_poiskkino(movie_type):
    """Преобразует тип из poiskkino в формат kinopoiskapiunofficial"""
    type_map = {
        'movie': 'FILM',
        'tv-series': 'TV_SERIES',
        'cartoon': 'FILM',  # мультфильмы как фильмы
        'animated-series': 'TV_SERIES',
        'anime': 'TV_SERIES'
    }
    return type_map.get(movie_type, 'FILM')


def extract_movie_info(link_or_id):
    """
    Извлекает информацию о фильме/сериале по ссылке или kp_id.
    
    Поддерживает:
    - Полную ссылку: https://www.kinopoisk.ru/film/123456/ или /series/
    - Просто kp_id как строку: "123456"
    - kp_id как int: 123456
    
    Возвращает dict с данными или None при ошибке.
    """
    logger.info(f"[POISKKINO EXTRACT MOVIE] ===== START: link_or_id={link_or_id}")

    try:
        kp_id = None
        is_series = False

        # Обработка входных данных
        if isinstance(link_or_id, int):
            kp_id = str(link_or_id)
        elif isinstance(link_or_id, str):
            link = link_or_id.strip()
            
            match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', link)
            if match:
                kp_id = match.group(2)
                is_series = match.group(1) == 'series'
            else:
                if link.isdigit():
                    kp_id = link
                else:
                    logger.warning(f"[POISKKINO EXTRACT MOVIE] Не распознана ссылка или ID: {link}")
                    return None
        else:
            logger.warning(f"[POISKKINO EXTRACT MOVIE] Неподдерживаемый тип данных: {type(link_or_id)}")
            return None

        if not kp_id:
            logger.warning("[POISKKINO EXTRACT MOVIE] Не удалось определить kp_id")
            return None

        logger.info(f"[POISKKINO EXTRACT MOVIE] kp_id={kp_id}, is_series={is_series}")

        headers = _get_headers()

        # В poiskkino API - один запрос возвращает всё включая persons
        url_main = f"{BASE_URL}/v1.4/movie/{kp_id}"
        logger.info(f"[POISKKINO EXTRACT MOVIE] Запрос к {url_main}")
        
        # Запрашиваем нужные поля
        params = {
            'selectFields': ['id', 'name', 'alternativeName', 'enName', 'year', 'description', 
                           'shortDescription', 'type', 'isSeries', 'genres', 'persons', 'facts',
                           'similarMovies', 'sequelsAndPrequels', 'watchability', 'premiere',
                           'releaseYears', 'seasonsInfo']
        }
        
        response_main = requests.get(url_main, headers=headers, timeout=15)
        log_poiskkino_api_request(f"/v1.4/movie/{kp_id}", 'GET', response_main.status_code, None, None, kp_id)
        
        if response_main.status_code != 200:
            logger.error(f"[POISKKINO EXTRACT MOVIE] Ошибка API: {response_main.status_code}, текст: {response_main.text[:200]}")
            return None
        
        data_main = response_main.json()

        # Определяем тип
        api_type = data_main.get('type', '')
        is_series_from_api = data_main.get('isSeries', False)
        if is_series_from_api or api_type in ['tv-series', 'animated-series', 'anime']:
            is_series = True
        elif api_type == 'movie':
            is_series = False

        title = data_main.get('name') or data_main.get('alternativeName') or data_main.get('enName') or "Unknown"
        year = data_main.get('year') or "—"
        
        # Жанры в poiskkino приходят как [{name: "драма"}, ...]
        genres_list = data_main.get('genres', [])
        genres = ', '.join([g.get('name', '') for g in genres_list if g.get('name')]) or "—"
        
        description = data_main.get('description') or data_main.get('shortDescription') or "Нет описания"

        # Persons уже в ответе
        persons = data_main.get('persons', [])
        
        # Режиссёр
        director = "Не указан"
        for person in persons:
            if not isinstance(person, dict):
                continue
            profession = person.get('enProfession') or person.get('profession') or ''
            if 'director' in str(profession).lower() or 'режиссер' in str(profession).lower():
                name = person.get('name') or person.get('enName')
                if name:
                    director = name
                    break

        # Актёры (top 6)
        actors_list = []
        for person in persons:
            if not isinstance(person, dict):
                continue
            profession = person.get('enProfession') or person.get('profession') or ''
            if ('actor' in str(profession).lower() or 'актер' in str(profession).lower() or 'актриса' in str(profession).lower()) and len(actors_list) < 6:
                name = person.get('name') or person.get('enName')
                if name:
                    actors_list.append(name)
        actors = ', '.join(actors_list) if actors_list else "—"

        logger.info(f"[POISKKINO EXTRACT MOVIE] Успешно: {title} ({year}), режиссёр: {director}, is_series={is_series}")

        result = {
            'kp_id': kp_id,
            'title': title,
            'year': year,
            'genres': genres,
            'director': director,
            'actors': actors,
            'description': description,
            'is_series': is_series
        }
        
        logger.info(f"[POISKKINO EXTRACT MOVIE] ===== END: успешно, kp_id={kp_id}, title={title}, is_series={is_series}")
        return result

    except Exception as e:
        logger.error(f"[POISKKINO EXTRACT MOVIE] ===== END: КРИТИЧЕСКАЯ ОШИБКА для link_or_id={link_or_id}: {e}", exc_info=True)
        return None


def get_film_distribution(kp_id):
    """Получает информацию о прокате фильма в России. 
    Возвращает None или {'date': date, 'date_str': str} только если дата выхода в будущем.
    """
    headers = _get_headers()
    url = f"{BASE_URL}/v1.4/movie/{kp_id}"
    params = {'selectFields': ['premiere']}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        if response.status_code != 200:
            return None
        
        data = response.json()
        premiere = data.get('premiere', {})
        
        if not premiere:
            return None
            
        # Проверяем дату премьеры в России
        russia_premiere = premiere.get('russia')
        if not russia_premiere:
            # Пробуем cinema или world
            russia_premiere = premiere.get('cinema') or premiere.get('world')
            
        if not russia_premiere:
            return None
            
        try:
            # Формат даты может быть ISO или DD.MM.YYYY
            if 'T' in str(russia_premiere):
                release_date = datetime.fromisoformat(russia_premiere.replace('Z', '+00:00')).date()
            else:
                release_date = datetime.strptime(russia_premiere, '%Y-%m-%d').date()
            
            if release_date > date.today():
                return {'date': release_date, 'date_str': release_date.strftime('%d.%m.%Y')}
        except Exception:
            pass
            
        return None
    except Exception as e:
        logger.warning(f"[POISKKINO DISTRIBUTION] Ошибка получения проката для {kp_id}: {e}")
        return None


def get_facts(kp_id):
    """Получает интересные факты о фильме"""
    headers = _get_headers()
    url = f"{BASE_URL}/v1.4/movie/{kp_id}"
    params = {'selectFields': ['facts']}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        log_poiskkino_api_request(f"/v1.4/movie/{kp_id}?selectFields=facts", 'GET', response.status_code, None, None, kp_id)
        
        if response.status_code == 200:
            data = response.json()
            facts = data.get('facts', [])
            
            if facts:
                facts_list = []
                bloopers_list = []
                
                for fact in facts:
                    fact_text = fact.get('value', '').strip()
                    fact_type = fact.get('type', 'FACT')
                    spoiler = fact.get('spoiler', False)
                    
                    if fact_text and not spoiler:
                        # Исправляем HTML-сущности
                        fact_text = fact_text.replace('&laquo;', '«').replace('&raquo;', '»').replace('&quot;', '"').replace('&amp;', '&')
                        if fact_type == 'BLOOPER':
                            bloopers_list.append((fact_type, fact_text))
                        else:
                            facts_list.append((fact_type, fact_text))
                
                text = "🤔 <b>Факты о фильме:</b>\n\n"
                
                if facts_list:
                    for fact_type, fact_text in facts_list[:3]:
                        text += f"• <b>Факты:</b> {fact_text}\n\n"
                
                if bloopers_list:
                    for fact_type, fact_text in bloopers_list[:3]:
                        text += f"• <b>Ошибки:</b> {fact_text}\n\n"
                
                return text if (facts_list or bloopers_list) else None
            else:
                return None
        else:
            logger.error(f"[POISKKINO] Ошибка get_facts: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"[POISKKINO] Ошибка get_facts: {e}", exc_info=True)
        return None


def get_seasons(kp_id, chat_id=None, user_id=None):
    """Получает информацию о сезонах сериала с отметками просмотренных"""
    headers = _get_headers()
    url = f"{BASE_URL}/v1.4/season"
    params = {
        'movieId': kp_id,
        'limit': 250,
        'selectFields': ['number', 'episodesCount', 'episodes', 'name', 'airDate']
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        log_poiskkino_api_request(f"/v1.4/season?movieId={kp_id}", 'GET', response.status_code, user_id, chat_id, kp_id)
        
        if response.status_code == 200:
            data = response.json()
            seasons = data.get('docs', [])
            
            if seasons:
                # Получаем информацию о просмотренных сериях
                watched_episodes = set()
                if chat_id and user_id:
                    with db_lock:
                        cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
                        row = cursor.fetchone()
                        if row:
                            film_id = row.get('id') if isinstance(row, dict) else (row[0] if row else None)
                            if film_id:
                                cursor.execute('''
                                    SELECT season_number, episode_number 
                                    FROM series_tracking 
                                    WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                                ''', (chat_id, film_id, user_id))
                                watched_rows = cursor.fetchall()
                                for w_row in watched_rows:
                                    season = w_row.get('season_number') if isinstance(w_row, dict) else w_row[0]
                                    episode = w_row.get('episode_number') if isinstance(w_row, dict) else w_row[1]
                                    watched_episodes.add((season, episode))

                now = datetime.now()
                
                next_episode = None
                next_episode_date = None
                is_airing = False
                
                for season in seasons:
                    episodes = season.get('episodes', [])
                    for ep in episodes:
                        release_str = ep.get('airDate', '') or ep.get('date', '')
                        if release_str and release_str != '—':
                            try:
                                release_date = None
                                for fmt in ['%Y-%m-%d', '%d.%m.%Y', '%Y-%m-%dT%H:%M:%S']:
                                    try:
                                        release_date = datetime.strptime(release_str.split('T')[0], fmt)
                                        break
                                    except:
                                        continue
                                
                                if release_date and release_date > now:
                                    if not next_episode_date or release_date < next_episode_date:
                                        next_episode_date = release_date
                                        next_episode = {
                                            'season': season.get('number', ''),
                                            'episode': ep.get('number', ''),
                                            'date': release_date
                                        }
                                        is_airing = True
                            except:
                                pass
                
                season_stats = {}
                for season in seasons:
                    number = season.get('number', '')
                    episodes = season.get('episodes', [])
                    watched_in_season = sum(1 for ep in episodes if (number, str(ep.get('number', ''))) in watched_episodes)
                    total_in_season = len(episodes)
                    season_stats[number] = {'watched': watched_in_season, 'total': total_in_season}
                
                text = "📺 <b>Сезоны сериала:</b>\n\n"
                for season in seasons:
                    number = season.get('number', '')
                    episodes = season.get('episodes', [])
                    stats = season_stats.get(number, {'watched': 0, 'total': len(episodes)})
                    
                    if stats['watched'] == stats['total'] and stats['total'] > 0:
                        status = "✅ Просмотрен"
                    elif stats['watched'] > 0:
                        status = "👁 Частично просмотрен"
                    else:
                        status = "⬜ Не просмотрен"
                    
                    text += f"Сезон {number} ({stats['total']} серий) — {status}\n"
                
                text += "\n"
                
                if is_airing and next_episode:
                    text += f"🟢 <b>Сериал выходит сейчас</b>\n"
                    text += f"📅 Следующая серия: Сезон {next_episode['season']}, Эпизод {next_episode['episode']} — {next_episode['date'].strftime('%d.%m.%Y')}\n\n"
                else:
                    text += f"🔴 <b>Сериал не выходит</b>\n\n"
                
                return text
            else:
                return None
        else:
            logger.error(f"[POISKKINO] Ошибка get_seasons: {response.status_code}, response: {response.text[:200]}")
            return None
    except Exception as e:
        logger.error(f"[POISKKINO] Ошибка get_seasons: {e}", exc_info=True)
        return None


def get_seasons_data(kp_id):
    """Получает данные о сезонах сериала (возвращает список сезонов)"""
    headers = _get_headers()
    url = f"{BASE_URL}/v1.4/season"
    params = {
        'movieId': kp_id,
        'limit': 250,
        'selectFields': ['number', 'episodesCount', 'episodes', 'name', 'airDate']
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            seasons = data.get('docs', [])
            # Преобразуем формат для совместимости с kinopoiskapiunofficial
            result = []
            for s in seasons:
                season = {
                    'number': s.get('number'),
                    'episodes': []
                }
                for ep in s.get('episodes', []):
                    season['episodes'].append({
                        'episodeNumber': ep.get('number'),
                        'releaseDate': ep.get('airDate') or ep.get('date', '')
                    })
                result.append(season)
            return result
        else:
            logger.error(f"[POISKKINO] Ошибка get_seasons_data: {response.status_code}")
            return []
    except Exception as e:
        logger.error(f"[POISKKINO] Ошибка get_seasons_data: {e}", exc_info=True)
        return []


def get_similars(kp_id):
    """Получает похожие фильмы с типом"""
    headers = _get_headers()
    url = f"{BASE_URL}/v1.4/movie/{kp_id}"
    params = {'selectFields': ['similarMovies']}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            similars = data.get('similarMovies', [])
            result = []
            for s in similars[:10]:
                film_id = s.get('id')
                name = s.get('name') or s.get('enName', 'Без названия')
                film_type = s.get('type', '').lower()
                is_series = film_type in ['tv-series', 'animated-series', 'anime']
                if film_id and name:
                    result.append((film_id, name, is_series))
            return result
        return []
    except Exception as e:
        logger.error(f"[POISKKINO] Ошибка get_similars: {e}", exc_info=True)
        return []


def get_sequels(kp_id):
    """Получает продолжения, приквелы и ремейки"""
    headers = _get_headers()
    url = f"{BASE_URL}/v1.4/movie/{kp_id}"
    params = {'selectFields': ['sequelsAndPrequels']}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            items = data.get('sequelsAndPrequels', [])
            
            sequels = []
            remakes = []
            
            for item in items:
                film_id = item.get('id')
                name = item.get('name') or item.get('enName', 'Без названия')
                relation_type = item.get('type', '').upper()
                
                if film_id and name:
                    # В poiskkino нет явного поля relationType как в kinopoiskapiunofficial
                    # Поэтому все считаем сиквелами/приквелами
                    sequels.append((film_id, name))
            
            return {
                'sequels': sequels[:5],
                'remakes': remakes[:5]
            }
        return {'sequels': [], 'remakes': []}
    except Exception as e:
        logger.error(f"[POISKKINO] Ошибка get_sequels: {e}", exc_info=True)
        return {'sequels': [], 'remakes': []}


def get_external_sources(kp_id):
    """Получает внешние источники для просмотра фильма/сериала"""
    try:
        kp_id = int(kp_id)
    except (ValueError, TypeError):
        logger.warning(f"[POISKKINO] Некорректный kp_id: {kp_id}")
        return []
    
    if kp_id <= 0:
        logger.warning(f"[POISKKINO] Некорректный kp_id: {kp_id}")
        return []
    
    headers = _get_headers()
    url = f"{BASE_URL}/v1.4/movie/{kp_id}"
    params = {'selectFields': ['watchability']}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        
        logger.info(f"[POISKKINO external_sources] kp_id={kp_id} | status={response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            watchability = data.get('watchability', {})
            items = watchability.get('items', []) if watchability else []
            
            logger.info(f"[POISKKINO external_sources] kp_id={kp_id} | найдено items: {len(items)}")
            
            links = []
            for s in items:
                platform = s.get('name', 'Смотреть онлайн')
                url = s.get('url')
                if url:
                    links.append((platform, url))
            
            return links
        else:
            logger.warning(f"[POISKKINO external_sources] kp_id={kp_id} | статус: {response.status_code}")
            return []
            
    except requests.exceptions.RequestException as e:
        logger.error(f"[POISKKINO external_sources] kp_id={kp_id} | сетевая ошибка: {e}", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"[POISKKINO external_sources] kp_id={kp_id} | ошибка: {e}", exc_info=True)
        return []


def get_film_filters():
    """Получает список жанров из API ПоискКино"""
    headers = _get_headers()
    url = f"{BASE_URL}/v1/movie/possible-values-by-field"
    params = {'field': 'genres.name'}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            # Формат: [{"name": "драма", "slug": "drama"}, ...]
            filtered_genres = []
            for i, genre_item in enumerate(data):
                genre_name = genre_item.get('name', '').strip()
                if genre_name and genre_name.lower() != 'для взрослых':
                    filtered_genres.append({
                        'id': i + 1,  # poiskkino использует имена, не id
                        'genre': genre_name
                    })
            return filtered_genres
        return []
    except Exception as e:
        logger.error(f"[POISKKINO] Ошибка get_film_filters: {e}", exc_info=True)
        return []


def search_films_by_filters(genres=None, film_type=None, year_from=None, year_to=None, page=1):
    """Поиск фильмов по фильтрам через API ПоискКино"""
    headers = _get_headers()
    url = f"{BASE_URL}/v1.4/movie"
    
    params = {
        'page': page,
        'limit': 20,
        'sortField': 'rating.kp',
        'sortType': '-1',  # По убыванию
        'selectFields': ['id', 'name', 'alternativeName', 'year', 'poster', 'rating', 'type', 'genres']
    }
    
    # Тип: movie, tv-series
    if film_type:
        type_map = {'FILM': 'movie', 'TV_SERIES': 'tv-series'}
        params['type'] = type_map.get(film_type, film_type.lower())
    
    # Жанры в poiskkino задаются по имени
    if genres is not None:
        # Получим список жанров для маппинга id -> name
        all_genres = get_film_filters()
        genre_map = {g['id']: g['genre'] for g in all_genres}
        
        if isinstance(genres, list):
            genre_id = genres[0] if genres else None
        else:
            genre_id = genres
        
        if genre_id:
            genre_name = genre_map.get(genre_id, '')
            if genre_name:
                params['genres.name'] = genre_name
    
    if year_from:
        params['year'] = f"{year_from}-{year_to or 3000}"
    elif year_to:
        params['year'] = f"1000-{year_to}"
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            items = data.get('docs', [])
            # Преобразуем формат для совместимости
            result = []
            for item in items:
                result.append({
                    'kinopoiskId': item.get('id'),
                    'filmId': item.get('id'),
                    'nameRu': item.get('name'),
                    'nameOriginal': item.get('alternativeName'),
                    'year': item.get('year'),
                    'posterUrlPreview': item.get('poster', {}).get('previewUrl') if item.get('poster') else None,
                    'ratingKinopoisk': item.get('rating', {}).get('kp') if item.get('rating') else None,
                    'type': _map_type_from_poiskkino(item.get('type', '')),
                    'genres': [{'genre': g.get('name')} for g in item.get('genres', [])]
                })
            return result
        logger.warning(f"[POISKKINO SEARCH FILMS] API вернул статус {response.status_code}")
        return []
    except Exception as e:
        logger.error(f"[POISKKINO] Ошибка search_films_by_filters: {e}", exc_info=True)
        return []


def get_premieres_for_period(period_type='current_month'):
    """Получает список премьер для указанного периода"""
    now = datetime.now()
    headers = _get_headers()
    
    # Определяем диапазон дат
    if period_type == 'current_month':
        start_date = now.replace(day=1)
        if now.month == 12:
            end_date = now.replace(year=now.year + 1, month=1, day=1)
        else:
            end_date = now.replace(month=now.month + 1, day=1)
    elif period_type == 'next_month':
        if now.month == 12:
            start_date = now.replace(year=now.year + 1, month=1, day=1)
            end_date = now.replace(year=now.year + 1, month=2, day=1)
        else:
            start_date = now.replace(month=now.month + 1, day=1)
            if now.month + 1 == 12:
                end_date = now.replace(year=now.year + 1, month=1, day=1)
            else:
                end_date = now.replace(month=now.month + 2, day=1)
    elif period_type == '3_months':
        start_date = now
        end_month = now.month + 3
        end_year = now.year
        while end_month > 12:
            end_month -= 12
            end_year += 1
        end_date = now.replace(year=end_year, month=end_month, day=1)
    elif period_type == '6_months':
        start_date = now
        end_month = now.month + 6
        end_year = now.year
        while end_month > 12:
            end_month -= 12
            end_year += 1
        end_date = now.replace(year=end_year, month=end_month, day=1)
    elif period_type == 'current_year':
        start_date = now
        end_date = now.replace(year=now.year + 1, month=1, day=1)
    elif period_type == 'next_year':
        start_date = now.replace(year=now.year + 1, month=1, day=1)
        end_date = now.replace(year=now.year + 2, month=1, day=1)
    else:
        start_date = now.replace(day=1)
        if now.month == 12:
            end_date = now.replace(year=now.year + 1, month=1, day=1)
        else:
            end_date = now.replace(month=now.month + 1, day=1)
    
    # Форматируем даты для API
    start_str = start_date.strftime('%d.%m.%Y')
    end_str = end_date.strftime('%d.%m.%Y')
    
    url = f"{BASE_URL}/v1.4/movie"
    params = {
        'premiere.russia': f"{start_str}-{end_str}",
        'limit': 250,
        'sortField': 'premiere.russia',
        'sortType': '1',
        'selectFields': ['id', 'name', 'alternativeName', 'year', 'poster', 'rating', 'genres', 'premiere', 'countries']
    }
    
    try:
        logger.info(f"[POISKKINO PREMIERES] Запрос к API: {url} с параметрами {params}")
        response = requests.get(url, headers=headers, params=params, timeout=15)
        logger.info(f"[POISKKINO PREMIERES] Статус ответа: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            items = data.get('docs', [])
            
            # Преобразуем формат для совместимости с kinopoiskapiunofficial
            premieres = []
            for item in items:
                premiere_data = item.get('premiere', {})
                russia_date = premiere_data.get('russia') or premiere_data.get('cinema') or premiere_data.get('world')
                
                premieres.append({
                    'kinopoiskId': item.get('id'),
                    'filmId': item.get('id'),
                    'nameRu': item.get('name'),
                    'nameOriginal': item.get('alternativeName'),
                    'nameEn': item.get('alternativeName'),
                    'year': item.get('year'),
                    'posterUrlPreview': item.get('poster', {}).get('previewUrl') if item.get('poster') else None,
                    'genres': [{'genre': g.get('name')} for g in item.get('genres', [])],
                    'countries': [{'country': c.get('name')} for c in item.get('countries', [])],
                    'premiereRu': russia_date
                })
            
            logger.info(f"[POISKKINO PREMIERES] Получено премьер: {len(premieres)}")
            return premieres
        else:
            logger.warning(f"[POISKKINO PREMIERES] Ошибка {response.status_code}: {response.text[:200]}")
            return []
    except Exception as e:
        logger.error(f"[POISKKINO PREMIERES] Ошибка: {e}", exc_info=True)
        return []


def get_premieres(year=None, month=None):
    """Получает список премьер на указанный месяц"""
    if not year:
        year = datetime.now().year
    if not month:
        month = datetime.now().month
    
    # Формируем диапазон дат для месяца
    start_date = f"01.{month:02d}.{year}"
    
    # Последний день месяца
    if month == 12:
        end_date = f"31.12.{year}"
    else:
        from calendar import monthrange
        last_day = monthrange(year, month)[1]
        end_date = f"{last_day:02d}.{month:02d}.{year}"
    
    headers = _get_headers()
    url = f"{BASE_URL}/v1.4/movie"
    params = {
        'premiere.russia': f"{start_date}-{end_date}",
        'limit': 250,
        'sortField': 'premiere.russia',
        'sortType': '1',
        'selectFields': ['id', 'name', 'alternativeName', 'year', 'poster', 'rating', 'genres', 'premiere', 'countries']
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            items = data.get('docs', [])
            
            premieres = []
            for item in items:
                premiere_data = item.get('premiere', {})
                russia_date = premiere_data.get('russia') or premiere_data.get('cinema') or premiere_data.get('world')
                
                premieres.append({
                    'kinopoiskId': item.get('id'),
                    'filmId': item.get('id'),
                    'nameRu': item.get('name'),
                    'nameOriginal': item.get('alternativeName'),
                    'year': item.get('year'),
                    'posterUrlPreview': item.get('poster', {}).get('previewUrl') if item.get('poster') else None,
                    'genres': [{'genre': g.get('name')} for g in item.get('genres', [])],
                    'countries': [{'country': c.get('name')} for c in item.get('countries', [])],
                    'premiereRu': russia_date
                })
            return premieres
        return []
    except Exception as e:
        logger.error(f"[POISKKINO PREMIERES] Ошибка: {e}", exc_info=True)
        return []


def search_films(query, page=1):
    """Поиск фильмов через PoisKino API"""
    if not POISKKINO_TOKEN:
        logger.error("[POISKKINO SEARCH] POISKKINO_TOKEN не установлен")
        return [], 0
    
    url = f"{BASE_URL}/v1.4/movie/search"
    params = {"query": query, "page": page, "limit": 20}
    headers = _get_headers()
    
    logger.info(f"[POISKKINO SEARCH] Запрос: query='{query}', page={page}, url={url}")
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=15)
        log_poiskkino_api_request(f"/v1.4/movie/search", 'GET', response.status_code, None, None, None)
        logger.info(f"[POISKKINO SEARCH] Статус ответа: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"[POISKKINO SEARCH] Ошибка API: статус {response.status_code}")
            return [], 0
        
        data = response.json()
        items = data.get("docs", [])
        total_pages = data.get("pages", 1)
        logger.info(f"[POISKKINO SEARCH] Найдено результатов: {len(items)}, всего страниц: {total_pages}")
        
        # Преобразуем формат для совместимости с kinopoiskapiunofficial
        result = []
        for item in items:
            result.append({
                'kinopoiskId': item.get('id'),
                'filmId': item.get('id'),
                'nameRu': item.get('name'),
                'nameEn': item.get('enName'),
                'nameOriginal': item.get('alternativeName'),
                'year': item.get('year'),
                'description': item.get('description'),
                'posterUrlPreview': item.get('poster', {}).get('previewUrl') if item.get('poster') else None,
                'posterUrl': item.get('poster', {}).get('url') if item.get('poster') else None,
                'ratingKinopoisk': item.get('rating', {}).get('kp') if item.get('rating') else None,
                'type': _map_type_from_poiskkino(item.get('type', '')),
                'genres': [{'genre': g.get('name')} for g in item.get('genres', [])]
            })
        
        return result, total_pages
    except requests.exceptions.RequestException as e:
        logger.error(f"[POISKKINO SEARCH] Ошибка запроса: {e}")
        return [], 0
    except Exception as e:
        logger.error(f"[POISKKINO SEARCH] Неожиданная ошибка: {e}", exc_info=True)
        return [], 0


def search_persons(query, page=1):
    """Поиск людей через PoisKino API /v1.4/person/search"""
    if not POISKKINO_TOKEN:
        logger.error("[POISKKINO SEARCH PERSONS] POISKKINO_TOKEN не установлен")
        return [], 0
    
    url = f"{BASE_URL}/v1.4/person/search"
    params = {"query": query, "page": page, "limit": 20}
    headers = _get_headers()
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=15)
        log_poiskkino_api_request("/v1.4/person/search", "GET", response.status_code, None, None, None)
        
        if response.status_code != 200:
            logger.error(f"[POISKKINO SEARCH PERSONS] API статус {response.status_code}")
            return [], 0
        
        data = response.json()
        items = data.get("docs", [])
        total = data.get("total", 0)
        total_pages = data.get("pages", 1)
        
        logger.info(f"[POISKKINO SEARCH PERSONS] Найдено: {len(items)}, total={total}")
        return items, total_pages
    except Exception as e:
        logger.error(f"[POISKKINO SEARCH PERSONS] Ошибка: {e}", exc_info=True)
        return [], 0


def get_staff(person_id):
    """Детальная информация о персоне: /v1.4/person/{personId}"""
    if not POISKKINO_TOKEN:
        logger.error("[POISKKINO GET STAFF] POISKKINO_TOKEN не установлен")
        return None
    
    url = f"{BASE_URL}/v1.4/person/{person_id}"
    headers = _get_headers()
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        log_poiskkino_api_request(f"/v1.4/person/{person_id}", "GET", response.status_code, None, None, person_id)
        
        if response.status_code != 200:
            logger.error(f"[POISKKINO GET STAFF] API статус {response.status_code}")
            return None
        
        data = response.json()
        
        # Преобразуем формат для совместимости с kinopoiskapiunofficial
        # В kinopoiskapiunofficial: personId, nameRu, nameEn, profession, films
        # В poiskkino: id, name, enName, profession, movies
        result = {
            'personId': data.get('id'),
            'nameRu': data.get('name'),
            'nameEn': data.get('enName'),
            'photo': data.get('photo'),
            'sex': data.get('sex'),
            'growth': data.get('growth'),
            'birthday': data.get('birthday'),
            'death': data.get('death'),
            'age': data.get('age'),
            'birthPlace': data.get('birthPlace', []),
            'deathPlace': data.get('deathPlace', []),
            'profession': [p.get('value') for p in data.get('profession', [])],
            'facts': [f.get('value') for f in data.get('facts', [])],
            'films': []
        }
        
        # Преобразуем movies в films
        for movie in data.get('movies', []):
            result['films'].append({
                'filmId': movie.get('id'),
                'nameRu': movie.get('name'),
                'nameEn': movie.get('alternativeName'),
                'rating': movie.get('rating'),
                'general': movie.get('general', False),
                'description': movie.get('description'),
                'professionKey': movie.get('enProfession')
            })
        
        return result
    except Exception as e:
        logger.error(f"[POISKKINO GET STAFF] Ошибка для person_id={person_id}: {e}", exc_info=True)
        return None


def get_film_by_imdb_id(imdb_id):
    """Получает информацию о фильме по IMDB ID"""
    headers = _get_headers()
    url = f"{BASE_URL}/v1.4/movie"
    params = {
        'externalId.imdb': imdb_id,
        'limit': 1,
        'selectFields': ['id', 'name', 'alternativeName', 'year', 'genres']
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        log_poiskkino_api_request(f"/v1.4/movie?externalId.imdb={imdb_id}", 'GET', response.status_code, None, None, None)
        
        if response.status_code == 200:
            data = response.json()
            items = data.get('docs', [])
            if items and len(items) > 0:
                film = items[0]
                kp_id = film.get('id')
                title = film.get('name') or film.get('alternativeName', 'Без названия')
                year = film.get('year')
                
                genres_list = film.get('genres', [])
                genres_ru = [g.get('name', '') for g in genres_list if g.get('name')]
                
                return {
                    'kp_id': str(kp_id) if kp_id else None,
                    'title': title,
                    'year': year,
                    'imdb_id': imdb_id,
                    'genres': genres_ru
                }
        
        logger.warning(f"[POISKKINO] Фильм с IMDB ID {imdb_id} не найден")
        return None
    except Exception as e:
        logger.error(f"[POISKKINO] Ошибка get_film_by_imdb_id для {imdb_id}: {e}", exc_info=True)
        return None
