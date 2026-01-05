# Инструкция по деплою и решению проблемы 409

## Проблема 409: Conflict: terminated by other getUpdates request

Эта ошибка возникает, когда несколько экземпляров бота пытаются одновременно получать обновления через `getUpdates`.

## Решение

### 1. Проверка процессов на сервере

Выполните на сервере:

```bash
# Проверка процессов
ps aux | grep -E "python.*moviebot|python.*main\.py|python -m moviebot" | grep -v grep

# Или используйте скрипт
./check_bot_processes.sh
```

### 2. Остановка старых процессов

```bash
# Автоматическая остановка
./check_bot_processes.sh

# Или вручную
pkill -f "python.*moviebot"
pkill -f "python.*main\.py"
pkill -f "python -m moviebot"
```

### 3. Проверка webhook

Убедитесь, что webhook не установлен (он конфликтует с polling):

```python
from moviebot.bot.bot_init import bot
bot.remove_webhook()
```

### 4. Проверка Procfile

Убедитесь, что `Procfile` содержит правильную команду:

```
web: python -m moviebot.main
```

**НЕ используйте:**
- `python moviebot.py` (старый файл)
- `python main.py` (если это не `moviebot/main.py`)
- `python moviebot.py.OLD_DO_NOT_USE` (старый файл)

### 5. Проверка на Heroku

Если используете Heroku:

```bash
# Проверка процессов
heroku ps

# Перезапуск всех dynos
heroku restart

# Проверка логов
heroku logs --tail
```

### 6. Проверка на Docker

Если используете Docker:

```bash
# Проверка контейнеров
docker ps | grep moviebot

# Остановка всех контейнеров
docker stop $(docker ps -q --filter "ancestor=moviebot")

# Перезапуск
docker-compose restart
```

### 7. Проверка systemd

Если используете systemd:

```bash
# Проверка сервисов
systemctl status moviebot

# Остановка сервиса
systemctl stop moviebot

# Перезапуск
systemctl restart moviebot
```

## Важные замечания

1. **Только один экземпляр**: Убедитесь, что запущен только ОДИН экземпляр бота
2. **Правильный entry point**: Используйте `python -m moviebot.main`, а не старые файлы
3. **Webhook vs Polling**: Не используйте webhook и polling одновременно
4. **Старые файлы**: Файл `moviebot.py.OLD_DO_NOT_USE` НЕ должен запускаться

## Автоматическое решение

Код автоматически:
- Очищает webhook перед запуском polling
- Делает несколько попыток с задержками при ошибке 409
- Логирует подробную информацию о проблеме

Если проблема сохраняется, проверьте логи и убедитесь, что:
1. Запущен только один экземпляр
2. Используется правильный entry point
3. Нет активных webhook

