# Заметки по рефакторингу

## Статус

Создана базовая структура проекта согласно требованиям:
- ✅ `moviebot/config.py` - конфигурация
- ✅ `moviebot/states.py` - состояния
- ✅ `moviebot/scheduler.py` - задачи планировщика
- ✅ Структура директорий создана

## Что осталось сделать

1. **Разбить moviebot.py на handlers:**
   - `bot/handlers/start.py` - команда /start
   - `bot/handlers/seasons.py` - команда /seasons
   - `bot/handlers/plan.py` - команда /plan
   - `bot/handlers/payment.py` - команда /payment
   - `bot/handlers/series.py` - работа с сериалами

2. **Создать callbacks:**
   - `bot/callbacks/series_callbacks.py`
   - `bot/callbacks/payment_callbacks.py`

3. **Создать commands.py** для регистрации всех handlers

4. **Создать main.py** как точку входа

5. **Обновить все импорты** в проекте

6. **Создать api/yookassa_api.py** для работы с платежами

## Важно

Файл `moviebot.py` содержит 24451 строку кода. Для полного рефакторинга необходимо:
- Извлечь все обработчики команд в соответствующие handlers
- Извлечь все callback handlers в callbacks
- Обновить все импорты
- Протестировать работоспособность

Старая версия сохранена в бекапе.

