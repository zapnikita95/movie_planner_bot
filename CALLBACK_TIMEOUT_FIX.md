# Исправление проблемы с долгими коллбеками (499 ошибки)

## Проблема

### Что такое 499 ошибка?
499 ошибка возникает, когда Telegram закрывает соединение до того, как сервер успел ответить. Это происходит потому, что:
1. Telegram требует ответа на webhook запрос в течение **60 секунд**
2. Если обработка callback query занимает больше времени, Telegram закрывает соединение
3. После этого весь бот может перестать работать, если обработка блокирует поток

### Почему это происходит?
- Тяжелые операции (запросы к API, обработка БД) выполняются **до** ответа на callback query
- Обработка update блокирует webhook endpoint
- Telegram не получает ответ вовремя и закрывает соединение

## Решение

### 1. Асинхронная обработка в webhook
**Файл:** `moviebot/web/web_app.py`

Обработка update теперь происходит в **отдельном потоке**, а webhook сразу возвращает 200 OK:

```python
# Запускаем обработку в отдельном потоке
process_thread = threading.Thread(target=process_update_async, daemon=True)
process_thread.start()

# Возвращаем 200 СРАЗУ, не дожидаясь обработки
return '', 200
```

Это предотвращает 499 ошибки, так как Telegram получает ответ сразу.

### 2. Быстрый ответ на callback queries
**Файл:** `moviebot/bot/bot_init.py` (функция `safe_answer_callback_query`)

Все callback handlers должны отвечать на callback query **СРАЗУ**, до начала тяжелых операций:

```python
# ПРАВИЛЬНО:
try:
    bot.answer_callback_query(call.id, text="⏳ Обрабатываю...")
    # Тяжелая обработка здесь
except Exception as e:
    # Обработка ошибок устаревших callback queries
    pass
```

### 3. Декоратор для callback handlers
**Файл:** `moviebot/utils/callback_decorator.py`

Создан декоратор `quick_callback_answer`, который автоматически:
- Отвечает на callback query сразу
- Обрабатывает ошибки устаревших callback queries
- Предотвращает падение бота при ошибках

**Использование:**
```python
from moviebot.utils.callback_decorator import quick_callback_answer
from moviebot.bot.bot_init import bot

@quick_callback_answer(bot)
@bot.callback_query_handler(func=lambda call: call.data.startswith("my_callback:"))
def my_handler(call):
    # Тяжелая обработка здесь
    pass
```

## Исправление проблемы с BOT_TOKEN

### Проблема
После обновления `BOT_TOKEN` в Railway переменных окружения бот не работает (401 Unauthorized).

### Причина
1. Приложение загружает `BOT_TOKEN` при старте
2. После обновления в Railway нужно **перезапустить приложение**
3. Переменные окружения не обновляются "на лету"

### Решение
1. **Обновите BOT_TOKEN в Railway:**
   - Перейдите в настройки проекта
   - Обновите переменную `BOT_TOKEN`
   - **ВАЖНО:** Перезапустите приложение (redeploy)

2. **Проверка токена:**
   - В логах теперь видно первые и последние символы токена для отладки
   - Если токен не загружен, будет критическая ошибка при старте

3. **Логирование:**
   ```
   [CONFIG] BOT_TOKEN загружен: 8554485843...tgJ1pQ
   [BOT INIT] Создание бота с токеном: 8554485843...tgJ1pQ
   ```

## Рекомендации

### Для всех callback handlers:
1. **Всегда отвечайте на callback query СРАЗУ:**
   ```python
   try:
       bot.answer_callback_query(call.id, text="⏳ Обрабатываю...")
   except Exception as e:
       # Обработка ошибок
       pass
   ```

2. **Используйте локальные соединения БД:**
   ```python
   from moviebot.database.db_connection import get_db_connection, get_db_cursor
   conn_local = get_db_connection()
   cursor_local = get_db_cursor()
   ```

3. **Обрабатывайте ошибки правильно:**
   ```python
   try:
       # Тяжелая обработка
   except Exception as e:
       logger.error(f"Ошибка: {e}", exc_info=True)
       # НЕ ПРОПАГИРУЕМ ОШИБКУ - это предотвращает падение бота
   ```

### Для тяжелых операций:
- Используйте фоновые потоки для долгих операций
- Отвечайте на callback query до начала тяжелой обработки
- Используйте асинхронную обработку где возможно

## Проверка

После применения исправлений:
1. Проверьте логи при старте - должен быть загружен BOT_TOKEN
2. Проверьте, что webhook возвращает 200 OK сразу
3. Проверьте, что callback handlers отвечают на callback queries сразу
4. Проверьте, что бот не падает при 499 ошибках

## Важно

- **После обновления BOT_TOKEN в Railway ОБЯЗАТЕЛЬНО перезапустите приложение!**
- Все callback handlers должны отвечать на callback query в течение первых 100ms
- Тяжелые операции должны выполняться после ответа на callback query
