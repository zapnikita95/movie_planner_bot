"""
API модуль для работы с Kinopoisk API

Этот модуль является фасадом для API менеджера, который поддерживает:
- Основной API: kinopoiskapiunofficial.tech или poiskkino.dev (настраивается через PRIMARY_API)
- Fallback при ошибках основного API
- Переключение между API через переменные окружения

Переменные окружения:
- PRIMARY_API: 'kinopoisk_unofficial' (по умолчанию) или 'poiskkino'
- FALLBACK_ENABLED: 'true' или 'false' (по умолчанию 'true')
- FALLBACK_THRESHOLD: количество ошибок для переключения (по умолчанию 20)
- FALLBACK_RESET_TIMEOUT: время сброса счётчика ошибок в секундах (по умолчанию 300)
- KP_TOKEN: токен для kinopoiskapiunofficial.tech
- POISKKINO_TOKEN: токен для poiskkino.dev
"""
import logging

# Импортируем функции из api_manager для обеспечения совместимости
from moviebot.api.api_manager import (
    extract_movie_info,
    get_film_distribution,
    get_facts,
    get_seasons,
    get_seasons_data,
    get_similars,
    get_sequels,
    get_external_sources,
    get_film_filters,
    search_films_by_filters,
    get_premieres_for_period,
    get_premieres,
    search_films,
    search_persons,
    get_staff,
    get_film_by_imdb_id,
    get_api_manager,
)

# Также импортируем функцию логирования из исходной реализации для совместимости
from moviebot.api.kinopoisk_api_impl import log_kinopoisk_api_request

logger = logging.getLogger(__name__)

# Экспортируем все публичные функции
__all__ = [
    'extract_movie_info',
    'get_film_distribution',
    'get_facts',
    'get_seasons',
    'get_seasons_data',
    'get_similars',
    'get_sequels',
    'get_external_sources',
    'get_film_filters',
    'search_films_by_filters',
    'get_premieres_for_period',
    'get_premieres',
    'search_films',
    'search_persons',
    'get_staff',
    'get_film_by_imdb_id',
    'log_kinopoisk_api_request',
    'get_api_manager',
    'get_api_status',
    'force_fallback',
    'reset_api_manager',
]


def get_api_status():
    """
    Возвращает текущий статус API менеджера.
    
    Returns:
        dict с ключами:
            - primary_api: основной API ('kinopoisk_unofficial' или 'poiskkino')
            - fallback_enabled: включён ли fallback
            - using_fallback: используется ли сейчас fallback
            - primary_error_count: количество последовательных ошибок основного API
            - fallback_threshold: порог для переключения на fallback
            - kp_token_available: доступен ли токен kinopoiskapiunofficial
            - poiskkino_token_available: доступен ли токен poiskkino
            - current_api: какой API используется сейчас
    """
    manager = get_api_manager()
    return manager.get_status()


def force_fallback(enable=True):
    """
    Принудительно включает или выключает использование fallback API.
    
    Args:
        enable: True для включения fallback, False для выключения
    """
    manager = get_api_manager()
    manager.force_fallback(enable)


def reset_api_manager():
    """
    Сбрасывает состояние API менеджера:
    - Обнуляет счётчик ошибок
    - Отключает fallback
    """
    manager = get_api_manager()
    manager.reset()
