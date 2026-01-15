"""
API модуль для работы с Kinopoisk API
"""
import re
import requests
import logging
from datetime import datetime
from moviebot.config import KP_TOKEN
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock

# Получаем глобальные объекты БД
conn = get_db_connection()
cursor = get_db_cursor()

logger = logging.getLogger(__name__)

def log_kinopoisk_api_request(endpoint, method='GET', status_code=None, user_id=None, chat_id=None, kp_id=None):
    """Логирует запрос к API Кинопоиска в БД"""
    # Используем локальные соединение и курсор для избежания проблем с закрытыми соединениями
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    with db_lock:
        try:
            # Самое важное: всегда чистим возможную сломанную транзакцию
            try:
                conn_local.rollback()
            except:
                pass

            # kp_id приводим к строке (потому что столбец text)
            kp_id_str = str(kp_id) if kp_id is not None else None

            cursor_local.execute('''
                INSERT INTO kinopoisk_api_logs 
                (endpoint, method, status_code, user_id, chat_id, kp_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (endpoint, method, status_code, user_id, chat_id, kp_id_str))
            
            conn_local.commit()
            
        except Exception as e:
            logger.error(f"Ошибка логирования API-запроса: {e}", exc_info=True)
            try:
                conn_local.rollback()
            except:
                pass  # если соединение уже мертво — молчим
        
def extract_movie_info(link_or_id):
    """
    Извлекает информацию о фильме/сериале по ссылке или kp_id.
    
    Поддерживает:
    - Полную ссылку: https://www.kinopoisk.ru/film/123456/ или /series/
    - Просто kp_id как строку: "123456"
    - kp_id как int: 123456
    
    Возвращает dict с данными или None при ошибке.
    """
    logger.info(f"[EXTRACT MOVIE] ===== START: link_or_id={link_or_id}")

    try:
        kp_id = None
        is_series = False

        # Обработка входных данных
        if isinstance(link_or_id, int):
            kp_id = str(link_or_id)
        elif isinstance(link_or_id, str):
            link = link_or_id.strip()
            
            # Пытаемся найти kp_id в ссылке
            match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', link)
            if match:
                kp_id = match.group(2)
                is_series = match.group(1) == 'series'
            else:
                # Если это просто число — считаем kp_id
                if link.isdigit():
                    kp_id = link
                else:
                    logger.warning(f"[EXTRACT MOVIE] Не распознана ссылка или ID: {link}")
                    return None
        else:
            logger.warning(f"[EXTRACT MOVIE] Неподдерживаемый тип данных: {type(link_or_id)}")
            return None

        if not kp_id:
            logger.warning("[EXTRACT MOVIE] Не удалось определить kp_id")
            return None

        logger.info(f"[EXTRACT MOVIE] kp_id={kp_id}, is_series={is_series}")

        headers = {
            'X-API-KEY': KP_TOKEN,
            'Content-Type': 'application/json'
        }

        # Основные данные (название, год, жанры, описание)
        url_main = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}"
        logger.info(f"[EXTRACT MOVIE] Запрос к {url_main}")
        response_main = requests.get(url_main, headers=headers, timeout=15)
        log_kinopoisk_api_request(f"/api/v2.2/films/{kp_id}", 'GET', response_main.status_code, None, None, kp_id)
        
        if response_main.status_code != 200:
            logger.error(f"[EXTRACT MOVIE] Ошибка API: {response_main.status_code}, текст: {response_main.text[:200]}")
            return None
        
        data_main = response_main.json()

        # Проверяем поле type в ответе API (более надежный способ определения типа)
        api_type = data_main.get('type', '').upper()
        if api_type == 'TV_SERIES':
            is_series = True
        elif api_type == 'FILM':
            is_series = False
        # Если type не указан, оставляем значение из URL (fallback)

        title = data_main.get('nameRu') or data_main.get('nameOriginal') or "Unknown"
        year = data_main.get('year') or "—"
        genres = ', '.join([g['genre'] for g in data_main.get('genres', [])]) or "—"
        description = data_main.get('description') or data_main.get('shortDescription') or "Нет описания"

        # Запрос на staff (режиссёр и актёры)
        url_staff = f"https://kinopoiskapiunofficial.tech/api/v1/staff?filmId={kp_id}"
        logger.debug(f"Staff запрос URL: {url_staff}")
        response_staff = requests.get(url_staff, headers=headers, timeout=15)
        log_kinopoisk_api_request(f"/api/v1/staff?filmId={kp_id}", 'GET', response_staff.status_code, None, None, kp_id)
        
        staff = []
        if response_staff.status_code == 200:
            staff = response_staff.json()
            logger.debug(f"Staff получено записей: {len(staff)}")
        else:
            logger.warning(f"Staff запрос ошибка {response_staff.status_code}")

        # Режиссёр
        director = "Не указан"
        for person in staff:
            if not isinstance(person, dict):
                continue
            profession = person.get('professionKey') or person.get('professionText') or person.get('profession')
            if profession and ('DIRECTOR' in str(profession).upper() or 'РЕЖИССЕР' in str(profession).upper()):
                name = person.get('nameRu') or person.get('nameEn') or person.get('name') or person.get('staffName')
                if name:
                    director = name
                    break

        # Актёры (top 6)
        actors_list = []
        for person in staff:
            if not isinstance(person, dict):
                continue
            profession = person.get('professionKey') or person.get('professionText') or person.get('profession')
            if profession and ('ACTOR' in str(profession).upper() or 'АКТЕР' in str(profession).upper()) and len(actors_list) < 6:
                name = person.get('nameRu') or person.get('nameEn') or person.get('name') or person.get('staffName')
                if name:
                    actors_list.append(name)
        actors = ', '.join(actors_list) if actors_list else "—"

        logger.info(f"[EXTRACT MOVIE] Успешно: {title} ({year}), режиссёр: {director}, is_series={is_series} (из API type={api_type})")

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
        
        logger.info(f"[EXTRACT MOVIE] ===== END: успешно, kp_id={kp_id}, title={title}, is_series={is_series}")
        return result

    except Exception as e:
        logger.error(f"[EXTRACT MOVIE] ===== END: КРИТИЧЕСКАЯ ОШИБКА для link_or_id={link_or_id}: {e}", exc_info=True)
        return None

def get_facts(kp_id):
    """Получает интересные факты о фильме"""
    headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/facts"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        log_kinopoisk_api_request(f"/api/v2.2/films/{kp_id}/facts", 'GET', response.status_code, None, None, kp_id)
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
                
                text = "🤔 <b>Факты о фильме:</b>\n\n"
                
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
        log_kinopoisk_api_request(f"/api/v2.2/films/{kp_id}/seasons", 'GET', response.status_code, user_id, chat_id, kp_id)
        if response.status_code == 200:
            data = response.json()
            seasons = data.get('items', [])
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
                        status = "✅ Просмотрен"
                    elif stats['watched'] > 0:
                        status = "👁 Частично просмотрен"
                    else:
                        status = "⬜ Не просмотрен"
                    
                    text += f"Сезон {number} ({stats['total']} серий) — {status}\n"
                
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
                            status = "✅ Просмотрен"
                        elif stats['watched'] > 0:
                            status = "👁 Частично просмотрен"
                        else:
                            status = "⬜ Не просмотрен"
                        
                        text += f"Сезон {number} ({stats['total']} серий) — {status}\n"
                    
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
    """Получает похожие фильмы с типом"""
    headers = {'X-API-KEY': KP_TOKEN}
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/similars"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            similars = data.get('items', [])
            result = []
            for s in similars[:10]:  # Берем больше, чтобы потом отфильтровать
                film_id = s.get('filmId')
                name = s.get('nameRu') or s.get('nameEn', 'Без названия')
                film_type = s.get('type', '').upper()
                is_series = film_type == 'TV_SERIES'
                if film_id and name:
                    result.append((film_id, name, is_series))
            return result
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
    """Получает внешние источники для просмотра фильма/сериала"""
    # Конвертируем kp_id в int, если это возможно
    try:
        kp_id = int(kp_id)
    except (ValueError, TypeError):
        logger.warning(f"Некорректный kp_id: {kp_id} (не может быть преобразован в число)")
        return []
    
    if kp_id <= 0:
        logger.warning(f"Некорректный kp_id: {kp_id} (должно быть > 0)")
        return []
    headers = {
        'X-API-KEY': KP_TOKEN,
        'Content-Type': 'application/json'
    }
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/external_sources"

    try:
        response = requests.get(url, headers=headers, timeout=15)
        
        logger.info(f"[external_sources] kp_id={kp_id} | status={response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            
            logger.info(f"[external_sources] kp_id={kp_id} | найдено items: {len(items)}")
            if items:
                # Логируем первые 2 для примера
                logger.info(f"Первые 2 источника: {items[:2]}")
            else:
                logger.info(f"[external_sources] kp_id={kp_id} → пустой список items")
            
            links = []
            for s in items:
                platform = s.get('platform', 'Смотреть онлайн')
                url = s.get('url')
                if url:
                    links.append((platform, url))
            
            return links
        
        else:
            logger.warning(f"[external_sources] kp_id={kp_id} | неожиданный статус: {response.status_code}")
            logger.debug(f"Ответ сервера: {response.text[:300]}...")  # первые 300 символов
            return []
            
    except requests.exceptions.RequestException as e:
        logger.error(f"[external_sources] kp_id={kp_id} | сетевая ошибка: {e}", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"[external_sources] kp_id={kp_id} | непредвиденная ошибка: {e}", exc_info=True)
        return []

def get_film_filters():
    """Получает список жанров из API Кинопоиска"""
    headers = {'X-API-KEY': KP_TOKEN, 'accept': 'application/json'}
    url = "https://kinopoiskapiunofficial.tech/api/v2.2/films/filters"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            genres = data.get('genres', [])
            # Фильтруем пустые жанры и "для взрослых"
            filtered_genres = []
            for genre_item in genres:
                genre_id = genre_item.get('id')
                genre_name = genre_item.get('genre', '').strip()
                # Пропускаем пустые жанры и "для взрослых"
                if genre_name and genre_name.lower() != 'для взрослых':
                    filtered_genres.append({
                        'id': genre_id,
                        'genre': genre_name
                    })
            return filtered_genres
        return []
    except Exception as e:
        logger.error(f"Ошибка get_film_filters: {e}", exc_info=True)
        return []


def search_films_by_filters(genres=None, film_type=None, year_from=None, year_to=None, page=1):
    """Поиск фильмов по фильтрам через API Кинопоиска
    
    Args:
        genres: ID жанра (число) или список ID жанров
        film_type: 'FILM' для фильмов, 'TV_SERIES' для сериалов, None для обоих типов
        year_from: Начальный год (по умолчанию 1000)
        year_to: Конечный год (по умолчанию 3000)
        page: Номер страницы (по умолчанию 1)
    """
    headers = {'X-API-KEY': KP_TOKEN, 'accept': 'application/json'}
    url = "https://kinopoiskapiunofficial.tech/api/v2.2/films"
    
    params = {
        'order': 'RATING',
        'ratingFrom': 0,
        'ratingTo': 10,
        'page': page
    }
    
    # Добавляем параметр type только если он указан (FILM или TV_SERIES)
    # Если None - не передаем type, получим оба типа
    if film_type:
        params['type'] = film_type  # FILM или TV_SERIES
    
    if genres is not None:
        # Если список жанров, берем первый (API не поддерживает несколько одновременно)
        if isinstance(genres, list):
            params['genres'] = genres[0] if genres and genres[0] else None
        else:
            # Если это число (id жанра), используем его напрямую
            params['genres'] = genres
    
    if year_from:
        params['yearFrom'] = year_from
    if year_to:
        params['yearTo'] = year_to
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        if response.status_code == 200:
            data = response.json()
            return data.get('items', [])
        logger.warning(f"[SEARCH FILMS] API вернул статус {response.status_code}: {response.text[:200]}")
        return []
    except Exception as e:
        logger.error(f"Ошибка search_films_by_filters: {e}", exc_info=True)
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
        log_kinopoisk_api_request(f"/api/v2.1/films/search-by-keyword", 'GET', response.status_code, None, None, None)
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


def get_film_by_imdb_id(imdb_id):
    """Получает информацию о фильме по IMDB ID"""
    headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
    
    # Используем поиск по IMDB ID
    url = "https://kinopoiskapiunofficial.tech/api/v2.2/films"
    params = {
        'order': 'RATING',
        'type': 'ALL',
        'ratingFrom': 0,
        'ratingTo': 10,
        'yearFrom': 1000,
        'yearTo': 3000,
        'imdbId': imdb_id,
        'page': 1
    }
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        log_kinopoisk_api_request(f"/api/v2.2/films?imdbId={imdb_id}", 'GET', response.status_code, None, None, None)
        
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            if items and len(items) > 0:
                film = items[0]
                kp_id = film.get('kinopoiskId') or film.get('filmId')
                title = film.get('nameRu') or film.get('nameOriginal', 'Без названия')
                year = film.get('year')
                
                return {
                    'kp_id': str(kp_id) if kp_id else None,
                    'title': title,
                    'year': year,
                    'imdb_id': imdb_id
                }
        
        logger.warning(f"Фильм с IMDB ID {imdb_id} не найден в Kinopoisk")
        return None
    except Exception as e:
        logger.error(f"Ошибка get_film_by_imdb_id для {imdb_id}: {e}", exc_info=True)
        return None

