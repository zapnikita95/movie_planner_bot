#!/usr/bin/env python3
"""
Универсальный скрипт для запуска всех тестов проекта
Автоматически находит и запускает все тесты из директории tests/
"""
import sys
import os
import unittest
from pathlib import Path

# Добавляем родительскую директорию в путь (movie_planner_bot, где находится moviebot)
script_dir = os.path.dirname(os.path.abspath(__file__))  # tests/
moviebot_dir = os.path.dirname(script_dir)  # moviebot/
parent_dir = os.path.dirname(moviebot_dir)  # movie_planner_bot/
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)


def discover_and_run_tests():
    """
    Автоматически находит и запускает все тесты из директории tests/
    """
    # Путь к директории с тестами
    tests_dir = script_dir
    
    print("=" * 70)
    print("🧪 ЗАПУСК ВСЕХ ТЕСТОВ ПРОЕКТА")
    print("=" * 70)
    print(f"\n📁 Директория тестов: {tests_dir}")
    print()
    
    # Используем unittest.TestLoader для автоматического обнаружения тестов
    loader = unittest.TestLoader()
    
    # Находим все тестовые файлы (test_*.py)
    test_suite = loader.discover(
        start_dir=tests_dir,
        pattern='test_*.py',
        top_level_dir=parent_dir
    )
    
    # Подсчитываем количество тестов перед запуском
    test_count = test_suite.countTestCases()
    
    if test_count == 0:
        print("⚠️  Тесты не найдены!")
        print(f"   Ищем файлы по паттерну: test_*.py в {tests_dir}")
        sys.exit(1)
    
    print(f"📊 Найдено тестов: {test_count}")
    print()
    print("-" * 70)
    print()
    
    # Запускаем тесты
    runner = unittest.TextTestRunner(
        verbosity=2,
        buffer=True,  # Буферизуем вывод для более чистого результата
        stream=sys.stdout
    )
    
    result = runner.run(test_suite)
    
    # Выводим статистику
    print()
    print("=" * 70)
    print("📈 СТАТИСТИКА ТЕСТОВ")
    print("=" * 70)
    print(f"✅ Тестов выполнено: {result.testsRun}")
    print(f"❌ Ошибок: {len(result.errors)}")
    print(f"⚠️  Провалов: {len(result.failures)}")
    print(f"⏭️  Пропущено: {len(result.skipped)}")
    
    if result.errors:
        print("\n" + "=" * 70)
        print("❌ ОШИБКИ:")
        print("=" * 70)
        for test, error in result.errors:
            print(f"\n🔴 {test}")
            print("-" * 70)
            # Выводим последние 500 символов ошибки
            error_lines = error.split('\n')
            if len(error_lines) > 30:
                print('\n'.join(error_lines[:15]))
                print("... (пропущено) ...")
                print('\n'.join(error_lines[-15:]))
            else:
                print(error)
    
    if result.failures:
        print("\n" + "=" * 70)
        print("⚠️  ПРОВАЛЫ:")
        print("=" * 70)
        for test, failure in result.failures:
            print(f"\n🟡 {test}")
            print("-" * 70)
            # Выводим последние 500 символов ошибки
            failure_lines = failure.split('\n')
            if len(failure_lines) > 30:
                print('\n'.join(failure_lines[:15]))
                print("... (пропущено) ...")
                print('\n'.join(failure_lines[-15:]))
            else:
                print(failure)
    
    if result.skipped:
        print("\n" + "=" * 70)
        print("⏭️  ПРОПУЩЕНО:")
        print("=" * 70)
        for test, reason in result.skipped:
            print(f"  {test}: {reason}")
    
    print()
    print("=" * 70)
    
    if result.wasSuccessful():
        print("✅ ВСЕ ТЕСТЫ ПРОШЛИ УСПЕШНО!")
        print("=" * 70)
        return 0
    else:
        print("❌ НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОШЛИ")
        print("=" * 70)
        return 1


if __name__ == '__main__':
    exit_code = discover_and_run_tests()
    sys.exit(exit_code)
