"""
Тесты для API модулей (kinopoisk_api, yookassa_api)
Покрытие: основные функции работы с внешними API
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Добавляем путь к проекту (родительская директория moviebot)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))  # movie_planner_bot/
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from moviebot.api import kinopoisk_api, yookassa_api


class TestKinopoiskAPI(unittest.TestCase):
    """Тесты для kinopoisk_api"""
    
    def setUp(self):
        """Настройка перед каждым тестом"""
        self.test_kp_id = "123456"
        self.test_link = "https://www.kinopoisk.ru/film/123456/"
        self.test_series_link = "https://www.kinopoisk.ru/series/789012/"
    
    @patch('moviebot.api.kinopoisk_api.log_kinopoisk_api_request')
    @patch('moviebot.api.kinopoisk_api.requests.get')
    def test_extract_movie_info_from_int(self, mock_get, mock_log):
        """Тест extract_movie_info - извлечение kp_id из int"""
        # Мокаем успешный ответ API
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'nameRu': 'Test Film',
            'nameOriginal': 'Test Film Original',
            'year': 2020,
            'genres': [{'genre': 'Action'}, {'genre': 'Drama'}],
            'description': 'Test description',
            'type': 'FILM'
        }
        mock_get.return_value = mock_response
        
        result = kinopoisk_api.extract_movie_info(123456)
        
        # Проверяем, что функция правильно извлекла kp_id
        mock_get.assert_called()
        # Функция должна была сделать запрос с kp_id="123456"
        calls = mock_get.call_args_list
        self.assertTrue(len(calls) > 0)
    
    @patch('moviebot.api.kinopoisk_api.log_kinopoisk_api_request')
    @patch('moviebot.api.kinopoisk_api.requests.get')
    def test_extract_movie_info_from_link(self, mock_get, mock_log):
        """Тест extract_movie_info - извлечение kp_id из ссылки"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'nameRu': 'Test Film',
            'year': 2020,
            'genres': [],
            'description': 'Test',
            'type': 'FILM'
        }
        mock_get.return_value = mock_response
        
        result = kinopoisk_api.extract_movie_info(self.test_link)
        
        # Проверяем, что функция сделала запрос
        self.assertTrue(mock_get.called)
    
    @patch('moviebot.api.kinopoisk_api.log_kinopoisk_api_request')
    @patch('moviebot.api.kinopoisk_api.requests.get')
    def test_extract_movie_info_from_series_link(self, mock_get, mock_log):
        """Тест extract_movie_info - извлечение kp_id из ссылки на сериал"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'nameRu': 'Test Series',
            'year': 2020,
            'genres': [],
            'description': 'Test',
            'type': 'TV_SERIES'
        }
        mock_get.return_value = mock_response
        
        result = kinopoisk_api.extract_movie_info(self.test_series_link)
        
        self.assertTrue(mock_get.called)
    
    def test_extract_movie_info_invalid_input(self):
        """Тест extract_movie_info - неправильный входной тип"""
        result = kinopoisk_api.extract_movie_info(None)
        self.assertIsNone(result)
        
        result = kinopoisk_api.extract_movie_info("invalid_link")
        self.assertIsNone(result)
    
    @patch('moviebot.api.kinopoisk_api.log_kinopoisk_api_request')
    @patch('moviebot.api.kinopoisk_api.requests.get')
    def test_extract_movie_info_api_error(self, mock_get, mock_log):
        """Тест extract_movie_info - обработка ошибки API"""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        mock_get.return_value = mock_response
        
        result = kinopoisk_api.extract_movie_info("123456")
        
        self.assertIsNone(result)
        mock_log.assert_called()
    
    @patch('moviebot.api.kinopoisk_api.log_kinopoisk_api_request')
    @patch('moviebot.api.kinopoisk_api.requests.get')
    def test_search_films_success(self, mock_get, mock_log):
        """Тест search_films - успешный поиск"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'films': [
                {
                    'filmId': 123,
                    'nameRu': 'Test Film',
                    'nameEn': 'Test Film',
                    'year': 2020
                }
            ],
            'totalPages': 1
        }
        mock_get.return_value = mock_response
        
        results, total_pages = kinopoisk_api.search_films("test query")
        
        self.assertEqual(total_pages, 1)
        self.assertTrue(len(results) > 0)
        mock_get.assert_called_once()
        mock_log.assert_called_once()
    
    @patch('moviebot.api.kinopoisk_api.requests.get')
    def test_search_films_api_error(self, mock_get):
        """Тест search_films - обработка ошибки API"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal Server Error"
        mock_get.return_value = mock_response
        
        results, total_pages = kinopoisk_api.search_films("test query")
        
        self.assertEqual(results, [])
        self.assertEqual(total_pages, 0)
    
    @patch('moviebot.api.kinopoisk_api.requests.get')
    def test_get_seasons_data_success(self, mock_get):
        """Тест get_seasons_data - успешное получение данных о сезонах"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'items': [
                {'number': 1, 'episodes': []},
                {'number': 2, 'episodes': []}
            ]
        }
        mock_get.return_value = mock_response
        
        result = kinopoisk_api.get_seasons_data(self.test_kp_id)
        
        self.assertEqual(len(result), 2)
        mock_get.assert_called_once()
    
    @patch('moviebot.api.kinopoisk_api.requests.get')
    def test_get_seasons_data_fallback_to_v21(self, mock_get):
        """Тест get_seasons_data - fallback на v2.1 при ошибке v2.2"""
        # Первый вызов (v2.2) - ошибка 400
        mock_response_v22 = Mock()
        mock_response_v22.status_code = 400
        
        # Второй вызов (v2.1) - успех
        mock_response_v21 = Mock()
        mock_response_v21.status_code = 200
        mock_response_v21.json.return_value = {
            'items': [{'number': 1}]
        }
        
        mock_get.side_effect = [mock_response_v22, mock_response_v21]
        
        result = kinopoisk_api.get_seasons_data(self.test_kp_id)
        
        self.assertEqual(len(result), 1)
        self.assertEqual(mock_get.call_count, 2)
    
    @patch('moviebot.api.kinopoisk_api.get_db_connection')
    @patch('moviebot.api.kinopoisk_api.get_db_cursor')
    @patch('moviebot.api.kinopoisk_api.db_lock')
    def test_log_kinopoisk_api_request(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест log_kinopoisk_api_request - логирование запроса в БД"""
        mock_cursor = Mock()
        mock_get_cursor.return_value = mock_cursor
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn
        
        kinopoisk_api.log_kinopoisk_api_request(
            endpoint="/test",
            method="GET",
            status_code=200,
            user_id=123,
            chat_id=456,
            kp_id=789
        )
        
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        # Функция использует локальные соединения, но close вызывается в блоке finally
        # Проверяем, что execute и commit были вызваны
        self.assertTrue(mock_cursor.execute.called)
        self.assertTrue(mock_conn.commit.called)


class TestYooKassaAPI(unittest.TestCase):
    """Тесты для yookassa_api"""
    
    def setUp(self):
        """Настройка перед каждым тестом"""
        self.test_shop_id = "test_shop_id"
        self.test_secret_key = "test_secret_key"
    
    @patch.dict(os.environ, {}, clear=True)
    @patch('moviebot.api.yookassa_api.YOOKASSA_AVAILABLE', False)
    def test_init_yookassa_not_available(self):
        """Тест init_yookassa - модуль yookassa недоступен"""
        result = yookassa_api.init_yookassa(self.test_shop_id, self.test_secret_key)
        self.assertFalse(result)
    
    @patch.dict(os.environ, {}, clear=True)
    @patch('moviebot.api.yookassa_api.Configuration')
    @patch('moviebot.api.yookassa_api.YOOKASSA_AVAILABLE', True)
    def test_init_yookassa_success(self, mock_config):
        """Тест init_yookassa - успешная инициализация"""
        result = yookassa_api.init_yookassa(self.test_shop_id, self.test_secret_key)
        
        # Проверяем, что Configuration была настроена
        self.assertEqual(mock_config.account_id, self.test_shop_id)
        self.assertEqual(mock_config.secret_key, self.test_secret_key)
    
    @patch.dict(os.environ, {}, clear=True)
    @patch('moviebot.api.yookassa_api.YOOKASSA_AVAILABLE', True)
    def test_init_yookassa_no_credentials(self):
        """Тест init_yookassa - отсутствие учетных данных"""
        result = yookassa_api.init_yookassa(None, None)
        self.assertFalse(result)
    
    @patch.dict(os.environ, {'YOOKASSA_SHOP_ID': 'env_shop_id', 'YOOKASSA_SECRET_KEY': 'env_secret_key'}, clear=False)
    @patch('moviebot.api.yookassa_api.Configuration')
    @patch('moviebot.api.yookassa_api.YOOKASSA_AVAILABLE', True)
    def test_init_yookassa_from_env(self, mock_config):
        """Тест init_yookassa - инициализация из переменных окружения"""
        result = yookassa_api.init_yookassa()
        
        self.assertEqual(mock_config.account_id, 'env_shop_id')
        self.assertEqual(mock_config.secret_key, 'env_secret_key')
    
    @patch('moviebot.api.yookassa_api.YOOKASSA_AVAILABLE', False)
    def test_create_payment_not_available(self):
        """Тест create_payment - модуль yookassa недоступен"""
        result = yookassa_api.create_payment(100.0, "Test payment")
        self.assertIsNone(result)
    
    @patch('moviebot.api.yookassa_api.YOOKASSA_AVAILABLE', True)
    @patch('moviebot.api.yookassa_api.Configuration')
    @patch('moviebot.api.yookassa_api.Payment')
    def test_create_payment_success(self, mock_payment, mock_config):
        """Тест create_payment - успешное создание платежа"""
        mock_config.account_id = self.test_shop_id
        mock_config.secret_key = self.test_secret_key
        
        mock_payment_obj = Mock()
        mock_payment_obj.id = "test_payment_id"
        mock_payment_obj.status = "pending"
        mock_payment.create.return_value = mock_payment_obj
        
        result = yookassa_api.create_payment(100.0, "Test payment")
        
        # Проверяем, что Payment.create был вызван
        mock_payment.create.assert_called_once()
        # Результат должен быть объектом платежа
        self.assertIsNotNone(result)


if __name__ == '__main__':
    unittest.main()
