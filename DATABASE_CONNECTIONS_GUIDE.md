# Руководство по работе с соединениями и курсорами БД

## Проблема: "cursor already closed"

### Суть проблемы

В проекте использовались **глобальные** соединения и курсоры (`conn`, `cursor`), которые создавались один раз при инициализации модуля. Это приводило к критическим ошибкам:

```
psycopg2.InterfaceError: cursor already closed
```

### Почему это происходило?

1. **Глобальные объекты** - `conn` и `cursor` создавались один раз и использовались везде
2. **Конкуренция потоков** - scheduler, обработчики сообщений и callback'и работают параллельно
3. **Преждевременное закрытие** - одна функция закрывала cursor, другая пыталась его использовать
4. **Транзакции** - `commit()` или `rollback()` в одной функции влияли на другие

## Решение: Локальные соединения

### Принцип работы

**Каждая функция, которая работает с БД, должна создавать свое собственное соединение и курсор.**

### Базовый паттерн

```python
def my_function():
    """Любая функция, работающая с БД"""
    # 1. Создаем локальное соединение
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        # 2. Работаем с БД внутри try блока
        with db_lock:
            cursor_local.execute("SELECT ...")
            result = cursor_local.fetchone()
            # ... другие операции
        
        # 3. Коммитим изменения (если нужно)
        conn_local.commit()
        
        return result
        
    except Exception as e:
        # 4. При ошибке откатываем транзакцию
        logger.error(f"Ошибка: {e}", exc_info=True)
        try:
            conn_local.rollback()
        except:
            pass
        raise  # или return None
        
    finally:
        # 5. ВСЕГДА закрываем соединение в finally
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass
```

## Правила работы с БД

### ✅ Правильно

#### 1. Локальное соединение для каждой функции

```python
def get_user_data(user_id):
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        with db_lock:
            cursor_local.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            return cursor_local.fetchone()
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass
```

#### 2. Отдельное соединение для каждой проверки в цикле

```python
def check_multiple_plans(plan_ids):
    """Если нужно проверить много планов, создаем соединение для каждой проверки"""
    for plan_id in plan_ids:
        conn_check = get_db_connection()
        cursor_check = get_db_cursor()
        try:
            with db_lock:
                cursor_check.execute("SELECT * FROM plans WHERE id = %s", (plan_id,))
                result = cursor_check.fetchone()
                # обработка результата
        finally:
            try:
                cursor_check.close()
            except:
                pass
            try:
                conn_check.close()
            except:
                pass
```

#### 3. Использование db_lock для критических секций

```python
def update_plan(plan_id, new_data):
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        # db_lock защищает от одновременного доступа
        with db_lock:
            cursor_local.execute(
                "UPDATE plans SET data = %s WHERE id = %s",
                (new_data, plan_id)
            )
            conn_local.commit()
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass
```

### ❌ Неправильно

#### 1. Использование глобального cursor

```python
# ❌ НИКОГДА ТАК НЕ ДЕЛАТЬ!
from moviebot.database.db_connection import cursor, conn

def bad_function():
    cursor.execute("SELECT ...")  # Может быть закрыт другой функцией!
    return cursor.fetchone()
```

#### 2. Использование cursor после вызова другой функции

```python
# ❌ НЕПРАВИЛЬНО
def bad_function():
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    # Вызываем другую функцию, которая может закрыть соединение
    other_function()  # Может создать свое соединение и повлиять на состояние
    
    # cursor_local может быть уже закрыт!
    cursor_local.execute("SELECT ...")  # ❌ ОШИБКА!
```

#### 3. Забыть закрыть соединение

```python
# ❌ НЕПРАВИЛЬНО - соединение не закрывается
def bad_function():
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    cursor_local.execute("SELECT ...")
    return cursor_local.fetchone()
    # Соединение остается открытым!
```

## Особые случаи

### Scheduler функции

Scheduler работает в отдельных потоках и может вызывать функции параллельно. **Всегда используйте локальные соединения:**

```python
def scheduler_function():
    """Функция, вызываемая scheduler"""
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        with db_lock:
            cursor_local.execute("SELECT ...")
            plans = cursor_local.fetchall()
        
        for plan in plans:
            # Для каждой проверки создаем отдельное соединение
            conn_check = get_db_connection()
            cursor_check = get_db_cursor()
            try:
                with db_lock:
                    cursor_check.execute("SELECT ...", (plan['id'],))
                    # проверка
            finally:
                try:
                    cursor_check.close()
                except:
                    pass
                try:
                    conn_check.close()
                except:
                    pass
                    
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass
```

### Функции, вызываемые из других функций

Если функция A вызывает функцию B, и обе работают с БД, каждая должна иметь свое соединение:

```python
def function_a():
    conn_a = get_db_connection()
    cursor_a = get_db_cursor()
    try:
        with db_lock:
            cursor_a.execute("SELECT ...")
            result = cursor_a.fetchone()
        
        # Вызываем другую функцию
        function_b()  # Создаст свое соединение
        
        # Продолжаем работу с cursor_a - он все еще валиден
        with db_lock:
            cursor_a.execute("UPDATE ...")
            conn_a.commit()
    finally:
        try:
            cursor_a.close()
        except:
            pass
        try:
            conn_a.close()
        except:
            pass

def function_b():
    # Создает свое собственное соединение
    conn_b = get_db_connection()
    cursor_b = get_db_cursor()
    try:
        with db_lock:
            cursor_b.execute("SELECT ...")
            # работа
    finally:
        try:
            cursor_b.close()
        except:
            pass
        try:
            conn_b.close()
        except:
            pass
```

### Фоновые потоки

Если функция запускается в отдельном потоке (например, через `threading.Thread`), она **обязательно** должна создавать свое соединение:

```python
def background_task():
    """Задача, выполняемая в фоне"""
    conn_bg = get_db_connection()
    cursor_bg = get_db_cursor()
    try:
        with db_lock:
            cursor_bg.execute("SELECT ...")
            # обработка
            conn_bg.commit()
    finally:
        try:
            cursor_bg.close()
        except:
            pass
        try:
            conn_bg.close()
        except:
            pass

# Запуск в фоне
thread = threading.Thread(target=background_task, daemon=True)
thread.start()
```

## db_lock - блокировка для критических секций

### Назначение

`db_lock` - это `threading.Lock()`, который предотвращает одновременное выполнение критических секций кода.

### Когда использовать

**Используйте `with db_lock:` когда:**
- Выполняете операции чтения/записи в БД
- Нужно гарантировать атомарность операции
- Работаете с несколькими таблицами в одной транзакции

### Примеры

```python
# ✅ Правильно - db_lock защищает операцию
def update_user(user_id, data):
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    try:
        with db_lock:
            cursor_local.execute("UPDATE users SET ... WHERE id = %s", (user_id,))
            cursor_local.execute("INSERT INTO logs ...", (user_id,))
            conn_local.commit()  # Обе операции в одной транзакции
    finally:
        # закрытие соединения
```

```python
# ✅ Правильно - db_lock для чтения в scheduler
def check_plans():
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    try:
        with db_lock:
            cursor_local.execute("SELECT * FROM plans WHERE ...")
            plans = cursor_local.fetchall()
        # Обработка plans вне db_lock
    finally:
        # закрытие соединения
```

## Чеклист для проверки кода

Перед коммитом проверьте:

- [ ] Функция создает локальное соединение (`conn_local = get_db_connection()`)
- [ ] Функция создает локальный курсор (`cursor_local = get_db_cursor()`)
- [ ] Все операции с БД обернуты в `try-except`
- [ ] Есть блок `finally` с закрытием соединения
- [ ] Используется `conn_local.commit()` вместо глобального `conn.commit()`
- [ ] Используется `cursor_local.execute()` вместо глобального `cursor.execute()`
- [ ] Критические операции обернуты в `with db_lock:`
- [ ] При ошибке выполняется `conn_local.rollback()`

## Миграция существующего кода

### Шаг 1: Найти использование глобального cursor

```bash
grep -r "cursor\.execute" moviebot/
grep -r "conn\.commit" moviebot/
```

### Шаг 2: Заменить на локальное соединение

**Было:**
```python
def old_function():
    with db_lock:
        cursor.execute("SELECT ...")
        return cursor.fetchone()
```

**Стало:**
```python
def old_function():
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    try:
        with db_lock:
            cursor_local.execute("SELECT ...")
            return cursor_local.fetchone()
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass
```

### Шаг 3: Проверить вложенные вызовы

Если функция вызывает другую функцию, работающую с БД, убедитесь, что каждая создает свое соединение.

## Дедлоки (Deadlocks)

### Проблема: Вложенные вызовы функций с db_lock

**ОПАСНО:** Если функция A использует `with db_lock:` и вызывает функцию B, которая тоже использует `with db_lock:`, может возникнуть дедлок.

### Пример дедлока

```python
# ❌ НЕПРАВИЛЬНО - может вызвать дедлок
def function_a():
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    try:
        with db_lock:  # Захватываем lock
            cursor_local.execute("SELECT ...")
            function_b()  # Вызывает функцию, которая тоже пытается захватить lock
            # ДЕДЛОК! function_b ждет освобождения lock, но мы его держим
    finally:
        # закрытие

def function_b():
    conn_b = get_db_connection()
    cursor_b = get_db_cursor()
    try:
        with db_lock:  # Пытается захватить lock, но он уже захвачен function_a
            cursor_b.execute("SELECT ...")
    finally:
        # закрытие
```

### Решение: Вызывать функции с db_lock ВНЕ критической секции

```python
# ✅ ПРАВИЛЬНО - вызываем функцию ВНЕ db_lock
def function_a():
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    try:
        with db_lock:
            cursor_local.execute("SELECT ...")
            data = cursor_local.fetchone()
        
        # Вызываем функцию ВНЕ db_lock
        function_b()  # Теперь безопасно - lock уже освобожден
        
        # Продолжаем работу с данными
        process_data(data)
    finally:
        # закрытие
```

### Реальный пример из проекта

**Проблема:** `get_film_current_state` вызывала `get_user_timezone_or_default` внутри `with db_lock:`, а `get_user_timezone` тоже использует `db_lock`.

**Решение:** Сохранить данные плана внутри `db_lock`, затем обработать их (включая вызов `get_user_timezone_or_default`) ВНЕ `db_lock`:

```python
# ✅ ПРАВИЛЬНО
def get_film_current_state(chat_id, kp_id, user_id):
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    try:
        with db_lock:
            # Получаем данные из БД
            cursor_local.execute("SELECT ...")
            plan_row = cursor_local.fetchone()
            plan_data = extract_plan_data(plan_row)  # Сохраняем данные
        
        # Обрабатываем данные ВНЕ db_lock
        if plan_data and user_id:
            # Вызываем get_user_timezone_or_default ВНЕ db_lock
            user_tz = get_user_timezone_or_default(user_id)  # Безопасно!
            # Форматируем дату
            date_str = format_date(plan_data['datetime'], user_tz)
    finally:
        # закрытие
```

## Частые ошибки и их решения

### Ошибка 1: "cursor already closed" в scheduler

**Причина:** Использование `cursor_local` после вызова функции, которая создает свое соединение.

**Решение:** Создавать отдельное соединение для каждой проверки.

### Ошибка 2: "cursor already closed" в обработчиках

**Причина:** Глобальный cursor используется в нескольких местах.

**Решение:** Всегда использовать локальные соединения.

### Ошибка 3: Меню не отправляется

**Причина:** Ошибка в функции получения подписки прерывает выполнение.

**Решение:** Обернуть вызовы в `try-except` и продолжать выполнение при ошибке.

### Ошибка 4: Функция зависает без ошибок

**Причина:** Дедлок - функция A держит `db_lock` и вызывает функцию B, которая тоже пытается захватить `db_lock`.

**Решение:** Вызывать функции, использующие `db_lock`, ВНЕ критической секции.

## Импорты

Всегда используйте правильные импорты:

```python
from moviebot.database.db_connection import (
    get_db_connection,
    get_db_cursor,
    db_lock
)
```

**НЕ импортируйте глобальные `conn` и `cursor`!**

## Резюме

1. **Каждая функция = свое соединение** - создавайте `conn_local` и `cursor_local` в каждой функции
2. **Всегда закрывайте соединение** - используйте `finally` блок
3. **Используйте db_lock** - для критических операций
4. **Обрабатывайте ошибки** - `try-except` с `rollback()` при ошибке
5. **Не используйте глобальные cursor/conn** - только локальные соединения

Следуя этим принципам, вы избежите ошибок "cursor already closed" и сделаете код более надежным и масштабируемым.
