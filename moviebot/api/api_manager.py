"""
API Manager - центральный менеджер для работы с API Кинопоиска
Поддерживает переключение между kinopoiskapiunofficial.tech и poiskkino.dev
с механизмом fallback при ошибках основного API
"""
import logging
import time
import threading
from functools import wraps

from moviebot.config import (
    PRIMARY_API, 
    FALLBACK_ENABLED, 
    FALLBACK_THRESHOLD,
    FALLBACK_RESET_TIMEOUT,
    KP_TOKEN,
    POISKKINO_TOKEN
)

logger = logging.getLogger(__name__)


class APIManager:
    """
    Singleton менеджер для работы с API Кинопоиска.
    Управляет переключением между основным и резервным API,
    отслеживает ошибки и реализует fallback логику.
    """
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._initialized = True
        
        # Определяем основной и резервный API
        self._primary_api = PRIMARY_API
        self._fallback_enabled = FALLBACK_ENABLED
        self._fallback_threshold = FALLBACK_THRESHOLD
        self._fallback_reset_timeout = FALLBACK_RESET_TIMEOUT
        
        # Счётчики ошибок
        self._primary_error_count = 0
        self._fallback_error_count = 0
        self._last_error_time = 0
        
        # Флаг использования fallback
        self._using_fallback = False
        
        # Блокировка для thread-safety
        self._state_lock = threading.Lock()
        
        # Проверяем доступность токенов
        self._kp_token_available = bool(KP_TOKEN)
        self._poiskkino_token_available = bool(POISKKINO_TOKEN)
        
        # Загружаем API модули
        self._load_api_modules()
        
        logger.info(f"[API Manager] Инициализирован. PRIMARY_API={self._primary_api}, "
                   f"FALLBACK_ENABLED={self._fallback_enabled}, "
                   f"FALLBACK_THRESHOLD={self._fallback_threshold}")
        logger.info(f"[API Manager] Токены: KP={self._kp_token_available}, "
                   f"POISKKINO={self._poiskkino_token_available}")
    
    def _load_api_modules(self):
        """Загружает модули API"""
        # Ленивая загрузка для избежания циклических импортов
        self._kp_api = None
        self._poiskkino_api = None
    
    def _get_kp_api(self):
        """Ленивая загрузка модуля kinopoiskapiunofficial API"""
        if self._kp_api is None:
            from moviebot.api import kinopoisk_api_impl as kp_api
            self._kp_api = kp_api
        return self._kp_api
    
    def _get_poiskkino_api(self):
        """Ленивая загрузка модуля poiskkino API"""
        if self._poiskkino_api is None:
            from moviebot.api import poiskkino_api as pk_api
            self._poiskkino_api = pk_api
        return self._poiskkino_api
    
    def _get_primary_module(self):
        """Возвращает модуль основного API"""
        if self._primary_api == 'poiskkino':
            if self._poiskkino_token_available:
                return self._get_poiskkino_api()
            elif self._kp_token_available:
                logger.warning("[API Manager] POISKKINO_TOKEN недоступен, использую kinopoiskapiunofficial")
                return self._get_kp_api()
        else:
            if self._kp_token_available:
                return self._get_kp_api()
            elif self._poiskkino_token_available:
                logger.warning("[API Manager] KP_TOKEN недоступен, использую poiskkino")
                return self._get_poiskkino_api()
        return None
    
    def _get_fallback_module(self):
        """Возвращает модуль резервного API"""
        if self._primary_api == 'poiskkino':
            if self._kp_token_available:
                return self._get_kp_api()
        else:
            if self._poiskkino_token_available:
                return self._get_poiskkino_api()
        return None
    
    def _reset_error_count_if_needed(self):
        """Сбрасывает счётчик ошибок, если прошло достаточно времени"""
        current_time = time.time()
        if current_time - self._last_error_time > self._fallback_reset_timeout:
            with self._state_lock:
                if self._primary_error_count > 0:
                    logger.info(f"[API Manager] Сброс счётчика ошибок (прошло {self._fallback_reset_timeout}с)")
                    self._primary_error_count = 0
                    self._using_fallback = False
    
    def record_error(self, is_primary=True):
        """Записывает ошибку API"""
        with self._state_lock:
            self._last_error_time = time.time()
            
            if is_primary:
                self._primary_error_count += 1
                logger.warning(f"[API Manager] Ошибка основного API ({self._primary_error_count}/{self._fallback_threshold})")
                
                if self._primary_error_count >= self._fallback_threshold and self._fallback_enabled:
                    if not self._using_fallback and self._get_fallback_module() is not None:
                        logger.warning(f"[API Manager] Переключение на fallback API после {self._primary_error_count} ошибок")
                        self._using_fallback = True
            else:
                self._fallback_error_count += 1
                logger.warning(f"[API Manager] Ошибка fallback API ({self._fallback_error_count})")
    
    def record_success(self, is_primary=True):
        """Записывает успешный запрос API"""
        with self._state_lock:
            if is_primary and self._primary_error_count > 0:
                # Уменьшаем счётчик ошибок при успехе
                self._primary_error_count = max(0, self._primary_error_count - 1)
    
    def is_using_fallback(self):
        """Возвращает True, если сейчас используется fallback API"""
        return self._using_fallback
    
    def get_current_api_name(self):
        """Возвращает название текущего используемого API"""
        if self._using_fallback:
            if self._primary_api == 'poiskkino':
                return 'kinopoiskapiunofficial'
            return 'poiskkino'
        return self._primary_api
    
    def get_active_module(self):
        """Возвращает активный модуль API с учётом fallback"""
        self._reset_error_count_if_needed()
        
        if self._using_fallback and self._fallback_enabled:
            fallback = self._get_fallback_module()
            if fallback:
                return fallback
        
        return self._get_primary_module()
    
    def force_fallback(self, enable=True):
        """Принудительно включает/выключает fallback"""
        with self._state_lock:
            self._using_fallback = enable
            logger.info(f"[API Manager] Fallback {'включён' if enable else 'выключен'} принудительно")
    
    def reset(self):
        """Сбрасывает состояние менеджера"""
        with self._state_lock:
            self._primary_error_count = 0
            self._fallback_error_count = 0
            self._using_fallback = False
            logger.info("[API Manager] Состояние сброшено")
    
    def get_status(self):
        """Возвращает текущий статус менеджера"""
        return {
            'primary_api': self._primary_api,
            'fallback_enabled': self._fallback_enabled,
            'using_fallback': self._using_fallback,
            'primary_error_count': self._primary_error_count,
            'fallback_threshold': self._fallback_threshold,
            'kp_token_available': self._kp_token_available,
            'poiskkino_token_available': self._poiskkino_token_available,
            'current_api': self.get_current_api_name()
        }


# Глобальный экземпляр менеджера
_manager = None


def get_api_manager():
    """Возвращает глобальный экземпляр API менеджера"""
    global _manager
    if _manager is None:
        _manager = APIManager()
    return _manager


def with_fallback(func_name):
    """
    Декоратор для функций API с поддержкой fallback.
    При ошибке основного API пытается выполнить запрос через резервный.
    
    Использование:
    @with_fallback('extract_movie_info')
    def extract_movie_info(link_or_id):
        ...
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            manager = get_api_manager()
            
            # Получаем активный модуль
            active_module = manager.get_active_module()
            if active_module is None:
                logger.error(f"[API Manager] Нет доступных API для {func_name}")
                return None
            
            # Получаем функцию из активного модуля
            api_func = getattr(active_module, func_name, None)
            if api_func is None:
                logger.error(f"[API Manager] Функция {func_name} не найдена в {active_module.__name__}")
                return None
            
            try:
                result = api_func(*args, **kwargs)
                
                # Проверяем результат - None или пустой список считаем потенциальной ошибкой
                # Но не записываем как ошибку, это может быть просто пустой результат
                if result is not None:
                    manager.record_success(not manager.is_using_fallback())
                
                return result
                
            except Exception as e:
                logger.error(f"[API Manager] Ошибка {func_name} на {manager.get_current_api_name()}: {e}")
                manager.record_error(not manager.is_using_fallback())
                
                # Если fallback включён и ещё не используется, пробуем fallback
                if manager._fallback_enabled and not manager.is_using_fallback():
                    fallback_module = manager._get_fallback_module()
                    if fallback_module:
                        fallback_func = getattr(fallback_module, func_name, None)
                        if fallback_func:
                            logger.info(f"[API Manager] Пробуем fallback для {func_name}")
                            try:
                                result = fallback_func(*args, **kwargs)
                                return result
                            except Exception as e2:
                                logger.error(f"[API Manager] Ошибка fallback {func_name}: {e2}")
                                manager.record_error(is_primary=False)
                
                return None
        
        return wrapper
    return decorator


# Публичные функции API с поддержкой fallback
# Эти функции будут экспортироваться из kinopoisk_api.py

def extract_movie_info(link_or_id):
    """Извлекает информацию о фильме/сериале"""
    manager = get_api_manager()
    module = manager.get_active_module()
    
    if module is None:
        logger.error("[API Manager] Нет доступных API")
        return None
    
    try:
        result = module.extract_movie_info(link_or_id)
        if result:
            manager.record_success(not manager.is_using_fallback())
        return result
    except Exception as e:
        logger.error(f"[API Manager] Ошибка extract_movie_info: {e}")
        manager.record_error(not manager.is_using_fallback())
        
        # Пробуем fallback
        if manager._fallback_enabled:
            fallback = manager._get_fallback_module()
            if fallback and fallback != module:
                try:
                    logger.info("[API Manager] Пробуем fallback для extract_movie_info")
                    return fallback.extract_movie_info(link_or_id)
                except Exception as e2:
                    logger.error(f"[API Manager] Fallback ошибка: {e2}")
        
        return None


def _call_with_fallback(func_name, *args, **kwargs):
    """Универсальная функция вызова API с fallback"""
    manager = get_api_manager()
    module = manager.get_active_module()
    
    if module is None:
        logger.error(f"[API Manager] Нет доступных API для {func_name}")
        return None
    
    func = getattr(module, func_name, None)
    if func is None:
        logger.error(f"[API Manager] Функция {func_name} не найдена")
        return None
    
    try:
        result = func(*args, **kwargs)
        if result is not None:
            manager.record_success(not manager.is_using_fallback())
        return result
    except Exception as e:
        logger.error(f"[API Manager] Ошибка {func_name}: {e}")
        manager.record_error(not manager.is_using_fallback())
        
        # Пробуем fallback
        if manager._fallback_enabled:
            fallback = manager._get_fallback_module()
            if fallback and fallback != module:
                fallback_func = getattr(fallback, func_name, None)
                if fallback_func:
                    try:
                        logger.info(f"[API Manager] Пробуем fallback для {func_name}")
                        return fallback_func(*args, **kwargs)
                    except Exception as e2:
                        logger.error(f"[API Manager] Fallback ошибка {func_name}: {e2}")
                        manager.record_error(is_primary=False)
        
        return None


def get_film_distribution(kp_id):
    return _call_with_fallback('get_film_distribution', kp_id)


def get_facts(kp_id):
    return _call_with_fallback('get_facts', kp_id)


def get_seasons(kp_id, chat_id=None, user_id=None):
    return _call_with_fallback('get_seasons', kp_id, chat_id, user_id)


def get_seasons_data(kp_id):
    return _call_with_fallback('get_seasons_data', kp_id)


def get_similars(kp_id):
    return _call_with_fallback('get_similars', kp_id)


def get_sequels(kp_id):
    return _call_with_fallback('get_sequels', kp_id)


def get_external_sources(kp_id):
    return _call_with_fallback('get_external_sources', kp_id)


def get_film_filters():
    return _call_with_fallback('get_film_filters')


def search_films_by_filters(genres=None, film_type=None, year_from=None, year_to=None, page=1):
    return _call_with_fallback('search_films_by_filters', genres, film_type, year_from, year_to, page)


def get_premieres_for_period(period_type='current_month'):
    return _call_with_fallback('get_premieres_for_period', period_type)


def get_premieres(year=None, month=None):
    return _call_with_fallback('get_premieres', year, month)


def search_films(query, page=1):
    manager = get_api_manager()
    module = manager.get_active_module()
    
    if module is None:
        return [], 0
    
    func = getattr(module, 'search_films', None)
    if func is None:
        return [], 0
    
    try:
        result = func(query, page)
        if result and result[0]:  # Если есть результаты
            manager.record_success(not manager.is_using_fallback())
        return result
    except Exception as e:
        logger.error(f"[API Manager] Ошибка search_films: {e}")
        manager.record_error(not manager.is_using_fallback())
        
        if manager._fallback_enabled:
            fallback = manager._get_fallback_module()
            if fallback and fallback != module:
                try:
                    return fallback.search_films(query, page)
                except:
                    pass
        
        return [], 0


def search_persons(query, page=1):
    manager = get_api_manager()
    module = manager.get_active_module()
    
    if module is None:
        return [], 0
    
    func = getattr(module, 'search_persons', None)
    if func is None:
        return [], 0
    
    try:
        result = func(query, page)
        if result and result[0]:
            manager.record_success(not manager.is_using_fallback())
        return result
    except Exception as e:
        logger.error(f"[API Manager] Ошибка search_persons: {e}")
        manager.record_error(not manager.is_using_fallback())
        
        if manager._fallback_enabled:
            fallback = manager._get_fallback_module()
            if fallback and fallback != module:
                try:
                    return fallback.search_persons(query, page)
                except:
                    pass
        
        return [], 0


def get_staff(person_id):
    return _call_with_fallback('get_staff', person_id)


def get_film_by_imdb_id(imdb_id):
    return _call_with_fallback('get_film_by_imdb_id', imdb_id)
