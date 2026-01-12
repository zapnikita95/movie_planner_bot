"""
Тесты для функций database/db_operations.py
Покрытие: основные функции работы с базой данных
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Добавляем путь к проекту (родительская директория moviebot)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from moviebot.database.db_operations import (
    get_watched_emoji,
    get_watched_emojis,
    get_watched_custom_emoji_ids,
    get_user_films_count,
    get_watched_reactions,
    get_ratings_info,
    has_subscription_feature,
    get_active_subscription_by_username,
    get_active_group_subscription,
    get_user_group_subscriptions,
    get_subscription_by_id,
    set_notification_setting,
    get_user_groups,
    is_bot_participant
)


class TestDBOperations(unittest.TestCase):
    """Тесты для функций db_operations"""
    
    def setUp(self):
        """Настройка перед каждым тестом"""
        self.test_chat_id = 123456789
        self.test_user_id = 987654321
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_get_watched_emoji_default(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест get_watched_emoji - возвращает дефолтное значение"""
        # Настраиваем моки
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        mock_get_cursor.return_value = mock_cursor
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn
        
        result = get_watched_emoji(self.test_chat_id)
        
        # Проверяем, что возвращается дефолтное значение
        self.assertIn("✅", result)
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_get_watched_emoji_from_db(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест get_watched_emoji - возвращает значение из БД"""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {'value': '🎬🎭'}
        mock_get_cursor.return_value = mock_cursor
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn
        
        result = get_watched_emoji(self.test_chat_id)
        
        self.assertEqual(result, '🎬🎭')
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_get_user_films_count(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест get_user_films_count"""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {'count': 5}
        mock_get_cursor.return_value = mock_cursor
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn
        
        result = get_user_films_count(self.test_user_id)
        
        self.assertEqual(result, 5)
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_get_user_films_count_zero(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест get_user_films_count - возвращает 0 если нет фильмов"""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        mock_get_cursor.return_value = mock_cursor
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn
        
        result = get_user_films_count(self.test_user_id)
        
        self.assertEqual(result, 0)
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_has_subscription_feature_creator(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест has_subscription_feature - специальный доступ для создателя"""
        # user_id создателя бота
        creator_id = 301810276
        
        result = has_subscription_feature(self.test_chat_id, creator_id, 'notifications')
        
        self.assertTrue(result)
        # Не должно быть вызовов к БД для создателя
        mock_get_cursor.assert_not_called()
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_has_subscription_feature_no_access(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест has_subscription_feature - нет доступа"""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None  # Нет персональной подписки
        mock_get_cursor.return_value = mock_cursor
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn
        
        result = has_subscription_feature(self.test_chat_id, self.test_user_id, 'notifications')
        
        self.assertFalse(result)
        mock_cursor.close.assert_called_once()
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_has_subscription_feature_personal(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест has_subscription_feature - есть персональная подписка"""
        mock_cursor = Mock()
        # Первый вызов - есть персональная подписка
        mock_cursor.fetchone.return_value = {'id': 1}
        mock_get_cursor.return_value = mock_cursor
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn
        
        result = has_subscription_feature(self.test_chat_id, self.test_user_id, 'notifications')
        
        self.assertTrue(result)
        mock_cursor.close.assert_called_once()
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_get_subscription_by_id(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест get_subscription_by_id"""
        subscription_id = 123
        mock_subscription = {'id': subscription_id, 'user_id': self.test_user_id}
        
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = mock_subscription
        mock_get_cursor.return_value = mock_cursor
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn
        
        result = get_subscription_by_id(subscription_id)
        
        self.assertEqual(result, mock_subscription)
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_get_user_groups_empty(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест get_user_groups - пустой список"""
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = []
        mock_get_cursor.return_value = mock_cursor
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn
        
        result = get_user_groups(self.test_user_id)
        
        self.assertEqual(result, [])
        mock_cursor.close.assert_called_once()
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_is_bot_participant_true(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест is_bot_participant - пользователь является участником"""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {'count': 1}
        mock_get_cursor.return_value = mock_cursor
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn
        
        result = is_bot_participant(self.test_chat_id, self.test_user_id)
        
        self.assertTrue(result)
        mock_cursor.close.assert_called_once()
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_is_bot_participant_false(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест is_bot_participant - пользователь не является участником"""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {'count': 0}
        mock_get_cursor.return_value = mock_cursor
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn
        
        result = is_bot_participant(self.test_chat_id, self.test_user_id)
        
        self.assertFalse(result)
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_get_ratings_info(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест get_ratings_info"""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = {'rating': 8}
        mock_get_cursor.return_value = mock_cursor
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn
        
        result = get_ratings_info(self.test_chat_id, 1, self.test_user_id)
        
        self.assertTrue(result['current_user_rated'])
        self.assertEqual(result['current_user_rating'], 8)
        mock_cursor.close.assert_called_once()
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_get_active_subscription_by_username_creator(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест get_active_subscription_by_username - создатель бота"""
        result = get_active_subscription_by_username('@zap_nikita', 'personal')
        
        self.assertIsNotNone(result)
        self.assertEqual(result['user_id'], 301810276)
        self.assertEqual(result['subscription_type'], 'personal')
        # Не должно быть вызовов к БД
        mock_get_cursor.assert_not_called()
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_get_active_group_subscription(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест get_active_group_subscription"""
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        mock_get_cursor.return_value = mock_cursor
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn
        
        result = get_active_group_subscription('test_group')
        
        self.assertIsNone(result)
        mock_cursor.close.assert_called_once()
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_get_user_group_subscriptions_creator(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест get_user_group_subscriptions - создатель бота"""
        creator_id = 301810276
        result = get_user_group_subscriptions(creator_id)
        
        self.assertEqual(result, [])
        mock_get_cursor.assert_not_called()
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_exception_handling(self, mock_lock, mock_get_cursor, mock_get_conn):
        """Тест обработки исключений - соединение закрывается даже при ошибке"""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("Database error")
        mock_get_cursor.return_value = mock_cursor
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn
        
        # Функция должна обработать исключение и закрыть соединение
        with self.assertRaises(Exception):
            get_user_films_count(self.test_user_id)
        
        # Проверяем, что соединение было закрыто
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
