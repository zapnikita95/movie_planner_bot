# Руководство по завершению рефакторинга

## Текущий статус

✅ **Базовая структура создана и готова к использованию**

### Что уже сделано:

1. ✅ Создан бекап проекта
2. ✅ Создана структура директорий `moviebot/`
3. ✅ Созданы базовые файлы:
   - `config.py`, `states.py`, `scheduler.py`, `main.py`
   - `bot/bot_init.py`, `bot/commands.py`
   - `bot/handlers/start.py` (полностью реализован)
   - `bot/handlers/list.py` (полностью реализован)
   - `bot/handlers/seasons.py` (структура создана)
   - `bot/handlers/plan.py` (структура создана)
   - `bot/handlers/payment.py` (структура создана)
   - `bot/handlers/series.py` (структура создана)
   - `bot/handlers/rate.py` (структура создана)
   - `bot/handlers/stats.py` (структура создана)
4. ✅ Обновлены импорты в существующих модулях
5. ✅ Создан `utils/helpers.py` с функциями проверки доступа

### Что осталось:

1. **Заполнить handlers** - извлечь код из `moviebot.py` (24451 строка)
2. **Создать callbacks модули**
3. **Создать api/yookassa_api.py**
4. **Обновить все импорты в handlers**
5. **Протестировать и запушить**

## Как продолжить работу

### Шаг 1: Заполнить handlers

Для каждого handler файла:

1. Откройте `moviebot.py` и найдите соответствующие функции
2. Скопируйте код в соответствующий handler файл
3. Обновите импорты на `moviebot.*`
4. Убедитесь, что функции принимают `bot` как параметр или получают его из контекста

**Пример для `handlers/seasons.py`:**

```python
# В moviebot.py строка 15645:
def seasons_command(message):
    # ... код функции ...

# В handlers/seasons.py:
def register_seasons_handlers(bot):
    @bot.message_handler(commands=['seasons'])
    def seasons_command(message):
        # ... скопированный код ...
        # Заменить bot на bot (уже есть в замыкании)
        # Заменить импорты на moviebot.*
```

### Шаг 2: Создать callbacks модули

Найдите все `@bot.callback_query_handler` в `moviebot.py` и разбейте их по логическим группам:

- `callbacks/series_callbacks.py` - все callback'и связанные с сериалами
- `callbacks/payment_callbacks.py` - все callback'и связанные с оплатой
- `callbacks/search_callbacks.py` - callback'и для поиска
- `callbacks/random_callbacks.py` - callback'и для рандома
- `callbacks/premieres_callbacks.py` - callback'и для премьер

### Шаг 3: Обновить импорты

После создания handlers обновите импорты:

1. В `bot/commands.py` - раскомментируйте регистрацию handlers
2. В handlers - обновите все импорты на `moviebot.*`
3. Убедитесь, что нет циклических импортов

### Шаг 4: Протестировать

1. Запустите `python moviebot/main.py`
2. Проверьте, что бот запускается
3. Протестируйте основные команды
4. Исправьте ошибки импортов

### Шаг 5: Запушить на GitHub

```bash
cd /Users/nikitazaporohzets/Desktop/Кино/movie_planner_bot
git add moviebot/
git commit -m "Рефакторинг: новая структура проекта"
git push
```

## Полезные команды для поиска кода

```bash
# Найти все обработчики команд
grep -n "@bot.message_handler(commands=" moviebot.py

# Найти все callback handlers
grep -n "@bot.callback_query_handler" moviebot.py

# Найти функцию по имени
grep -n "^def function_name" moviebot.py
```

## Структура после завершения

```
moviebot/
├── main.py                  ✅
├── config.py               ✅
├── states.py               ✅
├── scheduler.py            ✅
├── bot/
│   ├── bot_init.py         ✅
│   ├── commands.py         ✅
│   ├── handlers/          ✅ (структура готова)
│   │   ├── start.py        ✅
│   │   ├── list.py         ✅
│   │   ├── seasons.py      ⏳ (нужно заполнить)
│   │   ├── plan.py         ⏳ (нужно заполнить)
│   │   ├── payment.py      ⏳ (нужно заполнить)
│   │   ├── series.py       ⏳ (нужно заполнить)
│   │   ├── rate.py         ⏳ (нужно заполнить)
│   │   └── stats.py        ⏳ (нужно заполнить)
│   └── callbacks/          ⏳ (нужно создать)
├── database/               ✅
├── api/                     ✅
├── utils/                   ✅
├── web/                     ✅
└── services/               ✅
```

## Важные замечания

1. **Старый файл `moviebot.py` сохранен** - используйте его как справочник
2. **Все импорты должны использовать префикс `moviebot.`**
3. **Состояния находятся в `moviebot/states.py`**
4. **Конфигурация в `moviebot/config.py`**
5. **Планировщик в `moviebot/scheduler.py`**

## Следующие шаги

1. Начните с заполнения `handlers/seasons.py` - это относительно простой handler
2. Затем заполните `handlers/plan.py` - более сложный, но важный
3. Продолжите с остальными handlers
4. Создайте callbacks модули
5. Протестируйте и исправьте ошибки
6. Запушите на GitHub

