#!/usr/bin/env python3
"""
Скрипт для запуска тестов db_operations
Запускает тесты из правильной директории
"""
import sys
import os
import unittest

# Добавляем родительскую директорию в путь (movie_planner_bot, где находится moviebot)
script_dir = os.path.dirname(os.path.abspath(__file__))  # tests/
moviebot_dir = os.path.dirname(script_dir)  # moviebot/
parent_dir = os.path.dirname(moviebot_dir)  # movie_planner_bot/
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Теперь можно импортировать тесты
from moviebot.tests.test_db_operations import TestDBOperations

if __name__ == '__main__':
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestDBOperations)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print(f'\n{"="*60}')
    print(f'✅ Тестов выполнено: {result.testsRun}')
    print(f'❌ Ошибок: {len(result.errors)}')
    print(f'⚠️ Провалов: {len(result.failures)}')
    if result.errors:
        print('\nОшибки:')
        for test, error in result.errors:
            print(f'  {test}: {error[:200]}')
    if result.failures:
        print('\nПровалы:')
        for test, failure in result.failures:
            print(f'  {test}: {failure[:200]}')
    print("="*60)
    
    sys.exit(0 if result.wasSuccessful() else 1)
