# Тесты для database/db_operations.py

## Запуск тестов

### Способ 1: Из родительской директории (рекомендуется)

Из директории `movie_planner_bot`:

```bash
cd /Users/nikitazaporohzets/Desktop/Кино/movie_planner_bot
python3 -m unittest moviebot.tests.test_db_operations -v
```

### Способ 2: Используя скрипт run_tests.py

Из директории `movie_planner_bot`:

```bash
cd /Users/nikitazaporohzets/Desktop/Кино/movie_planner_bot
python3 moviebot/tests/run_tests.py
```

Или через прямой вызов Python:

```bash
cd /Users/nikitazaporohzets/Desktop/Кино/movie_planner_bot/moviebot
python3 -c "
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath('.')))
import unittest
from tests.test_db_operations import TestDBOperations
suite = unittest.TestLoader().loadTestsFromTestCase(TestDBOperations)
runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(suite)
print(f'\n✅ Тестов выполнено: {result.testsRun}')
print(f'❌ Ошибок: {len(result.errors)}')
print(f'⚠️ Провалов: {len(result.failures)}')
"
```

## Покрытие тестами

Текущие тесты покрывают следующие функции:

1. ✅ `get_watched_emoji` - 2 теста
2. ✅ `get_user_films_count` - 2 теста
3. ✅ `has_subscription_feature` - 3 теста (включая специальный доступ)
4. ✅ `get_subscription_by_id` - 1 тест
5. ✅ `get_user_groups` - 1 тест
6. ✅ `is_bot_participant` - 2 теста
7. ✅ `get_ratings_info` - 1 тест
8. ✅ `get_active_subscription_by_username` - 1 тест (создатель)
9. ✅ `get_active_group_subscription` - 1 тест
10. ✅ `get_user_group_subscriptions` - 1 тест (создатель)
11. ✅ Обработка исключений - 1 тест

**Всего: 16 тестов, все проходят успешно ✅**

## Что тестируется

- Корректное использование локальных соединений
- Закрытие соединений в блоке `finally`
- Обработка исключений
- Специальные случаи (создатель бота, пустые результаты)
- Дефолтные значения

## Для расширения покрытия

Для достижения 70%+ покрытия можно добавить тесты для:

- Функций записи (`create_subscription`, `renew_subscription`, `cancel_subscription`, `save_payment`, etc.)
- Более сложных сценариев (`has_subscription_feature` с групповыми подписками)
- Граничных случаев
- Интеграционные тесты с реальной БД (опционально)
