# Тесты проекта

## Запуск всех тестов

### ⭐ Рекомендуемый способ: Универсальный скрипт `run_all_tests.py`

Автоматически находит и запускает **все** тесты из директории `tests/`:

```bash
cd /Users/nikitazaporohzets/Desktop/Кино/movie_planner_bot
python3 moviebot/tests/run_all_tests.py
```

**Преимущества:**
- ✅ Автоматически находит все тестовые файлы (`test_*.py`)
- ✅ Выводит красивую статистику
- ✅ Легко расширяется для новых тестов
- ✅ Показывает детальную информацию об ошибках

### Альтернативные способы

#### Способ 1: Через unittest (все тесты)

Из директории `movie_planner_bot`:

```bash
cd /Users/nikitazaporohzets/Desktop/Кино/movie_planner_bot
python3 -m unittest discover -s moviebot/tests -p 'test_*.py' -v
```

#### Способ 2: Конкретный тестовый файл

```bash
cd /Users/nikitazaporohzets/Desktop/Кино/movie_planner_bot
python3 -m unittest moviebot.tests.test_db_operations -v
```

#### Способ 3: Старый скрипт run_tests.py (только test_db_operations)

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

## Тестовые файлы

В проекте есть следующие тестовые файлы:

1. **test_db_operations.py** - тесты для database/db_operations.py
2. **test_api.py** - тесты для API модулей (kinopoisk_api, yookassa_api)
3. **test_handlers.py** - тесты для handlers модулей

## Покрытие тестами

### test_db_operations.py

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

### test_api.py

Тесты покрывают следующие функции API модулей:

**kinopoisk_api:**
- ✅ `extract_movie_info` - извлечение информации из ссылок и ID (4 теста)
- ✅ `search_films` - поиск фильмов (2 теста)
- ✅ `get_seasons_data` - получение данных о сезонах (2 теста)
- ✅ `log_kinopoisk_api_request` - логирование запросов (1 тест)

**yookassa_api:**
- ✅ `init_yookassa` - инициализация YooKassa (4 теста)
- ✅ `create_payment` - создание платежа (2 теста)

**Всего: 15 тестов для API модулей**

### test_handlers.py

Тесты покрывают handlers модули:

- ✅ Проверка импортов handlers (3 теста)
- ✅ Структура start handlers (2 теста)
- ✅ Регистрация handlers (2 теста)
- ✅ Вспомогательные функции handlers (2 теста)

**Всего: 9 тестов для handlers**

## Общая статистика

- **test_db_operations.py**: 16 тестов
- **test_api.py**: 15 тестов
- **test_handlers.py**: 9 тестов
- **Итого: 40+ тестов** ✅
