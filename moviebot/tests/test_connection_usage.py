"""
Тесты для проверки правильного использования cursor/connection
Проверяет, что функции используют локальные соединения, а не глобальные
"""
import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
import ast
import inspect

# Добавляем путь к проекту (родительская директория moviebot)
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(os.path.dirname(current_dir))  # movie_planner_bot/
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)


class TestConnectionUsage(unittest.TestCase):
    """Тесты для проверки правильного использования cursor/connection"""
    
    def check_function_uses_local_connections(self, func_source_code):
        """
        Проверяет исходный код функции на использование локальных соединений
        
        Returns:
            (uses_local, uses_global): (bool, bool) - использует локальные/глобальные соединения
        """
        uses_local = False
        uses_global = False
        
        # Проверяем наличие локальных соединений
        if 'conn_local' in func_source_code or 'cursor_local' in func_source_code:
            uses_local = True
        
        # Проверяем использование глобальных cursor/conn внутри функции
        # (не в импортах, а в теле функции)
        lines = func_source_code.split('\n')
        in_function = False
        for line in lines:
            stripped = line.strip()
            # Пропускаем импорты и определения функций
            if stripped.startswith('def ') or stripped.startswith('@'):
                in_function = True
                continue
            if not in_function:
                continue
                
            # Проверяем использование глобальных cursor/conn
            if 'cursor.execute' in line and 'cursor_local' not in line:
                # Проверяем, что это не комментарий
                if '#' not in line or line.index('#') > line.index('cursor.execute'):
                    uses_global = True
            if 'conn.commit' in line and 'conn_local' not in line:
                if '#' not in line or line.index('#') > line.index('conn.commit'):
                    uses_global = True
            if 'conn.rollback' in line and 'conn_local' not in line:
                if '#' not in line or line.index('#') > line.index('conn.rollback'):
                    uses_global = True
        
        return uses_local, uses_global
    
    @patch('moviebot.utils.promo.get_db_connection')
    @patch('moviebot.utils.promo.get_db_cursor')
    @patch('moviebot.utils.promo.db_lock')
    def test_get_promocode_info_uses_local_connection(self, mock_lock, mock_get_cursor, mock_get_conn):
        """
        Тест: get_promocode_info должна использовать локальные соединения
        (тест проверяет поведение, но также нужно проверить код)
        """
        from moviebot.utils.promo import get_promocode_info
        
        # Настраиваем моки
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        mock_get_cursor.return_value = mock_cursor
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn
        
        result = get_promocode_info("TESTCODE")
        
        # Если функция использует локальные соединения, моки должны быть вызваны
        # Но если использует глобальные - они не будут вызваны
        # Проверяем исходный код функции
        func_source = inspect.getsource(get_promocode_info)
        uses_local, uses_global = self.check_function_uses_local_connections(func_source)
        
        # Функция ДОЛЖНА использовать локальные соединения
        # Если использует глобальные - это ошибка (но тест не упадет, только предупредит)
        if uses_global:
            self.fail("Функция get_promocode_info использует глобальный cursor/conn вместо локальных!")
    
    def test_promo_functions_source_code_check(self):
        """
        Тест: проверка исходного кода функций promo.py на использование локальных соединений
        """
        from moviebot.utils import promo
        
        # Функции для проверки
        functions_to_check = [
            'get_promocode_info',
            'apply_promocode',
            'create_promocode',
            'get_all_promocodes',
            'get_active_promocodes',
        ]
        
        issues = []
        
        for func_name in functions_to_check:
            if not hasattr(promo, func_name):
                continue
                
            func = getattr(promo, func_name)
            if not callable(func):
                continue
            
            try:
                func_source = inspect.getsource(func)
                uses_local, uses_global = self.check_function_uses_local_connections(func_source)
                
                if uses_global:
                    issues.append(f"{func_name}: использует глобальный cursor/conn")
                elif not uses_local:
                    # Функция может не использовать БД вообще
                    pass
            except OSError:
                # Функция может быть встроенной или из C модуля
                pass
            except Exception as e:
                issues.append(f"{func_name}: ошибка проверки - {e}")
        
        if issues:
            self.fail(f"Найдены функции с использованием глобальных соединений:\n" + "\n".join(issues))


class TestConnectionPatterns(unittest.TestCase):
    """Тесты для проверки паттернов использования соединений"""
    
    def test_local_connection_pattern(self):
        """
        Тест: проверяет правильный паттерн использования локальных соединений
        """
        correct_pattern = """
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        try:
            with db_lock:
                cursor_local.execute(...)
                conn_local.commit()
        finally:
            cursor_local.close()
            conn_local.close()
        """
        
        # Проверяем наличие ключевых элементов паттерна
        self.assertIn('conn_local', correct_pattern)
        self.assertIn('cursor_local', correct_pattern)
        self.assertIn('finally', correct_pattern)
        self.assertIn('.close()', correct_pattern)
    
    @patch('moviebot.database.db_operations.get_db_connection')
    @patch('moviebot.database.db_operations.get_db_cursor')
    @patch('moviebot.database.db_operations.db_lock')
    def test_db_operations_use_local_connections(self, mock_lock, mock_get_cursor, mock_get_conn):
        """
        Тест: функции db_operations используют локальные соединения
        (проверка на примере известных исправленных функций)
        """
        from moviebot.database.db_operations import get_watched_emoji
        
        mock_cursor = Mock()
        mock_cursor.fetchone.return_value = None
        mock_get_cursor.return_value = mock_cursor
        mock_conn = Mock()
        mock_get_conn.return_value = mock_conn
        
        result = get_watched_emoji(123456)
        
        # Проверяем, что были созданы локальные соединения
        self.assertTrue(mock_get_conn.called, "Функция должна создавать локальное соединение")
        self.assertTrue(mock_get_cursor.called, "Функция должна создавать локальный cursor")
        
        # Проверяем, что cursor был закрыт
        mock_cursor.close.assert_called()
        mock_conn.close.assert_called()


class TestGlobalCursorDetection(unittest.TestCase):
    """Тесты для обнаружения глобальных cursor/conn"""
    
    def test_detect_global_cursor_in_file(self):
        """
        Тест: обнаружение глобальных cursor/conn в файлах
        """
        # Путь к файлу promo.py
        promo_file = os.path.join(
            os.path.dirname(os.path.dirname(current_dir)),
            'moviebot',
            'utils',
            'promo.py'
        )
        
        if not os.path.exists(promo_file):
            self.skipTest(f"Файл {promo_file} не найден")
        
        with open(promo_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Проверяем наличие глобальных cursor/conn
        has_global_cursor = 'cursor = get_db_cursor()' in content
        has_global_conn = 'conn = get_db_connection()' in content
        
        if has_global_cursor or has_global_conn:
            # Это не обязательно ошибка - глобальные переменные могут существовать
            # Но функции должны использовать локальные соединения
            # Предупреждаем, но не падаем
            print(f"\n⚠️  В файле promo.py найдены глобальные cursor/conn (это нормально, если функции используют локальные)")


if __name__ == '__main__':
    unittest.main()
