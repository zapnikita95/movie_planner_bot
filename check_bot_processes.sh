#!/bin/bash
# Скрипт для проверки и остановки старых экземпляров бота на сервере

echo "🔍 Поиск процессов бота..."

# Ищем процессы Python, связанные с ботом
PROCESSES=$(ps aux | grep -E "python.*moviebot|python.*main\.py|python -m moviebot" | grep -v grep | grep -v "kill_old_bots" | grep -v "check_bot_processes")

if [ -z "$PROCESSES" ]; then
    echo "✅ Старые процессы бота не найдены"
    exit 0
fi

echo "📋 Найдены следующие процессы:"
echo "$PROCESSES"
echo ""

# Извлекаем PIDs
PIDS=$(echo "$PROCESSES" | awk '{print $2}')

echo "🛑 Остановка процессов..."
for PID in $PIDS; do
    echo "Останавливаю процесс $PID..."
    kill -TERM $PID 2>/dev/null
    sleep 1
    # Проверяем, завершился ли процесс
    if kill -0 $PID 2>/dev/null; then
        echo "Процесс $PID не завершился, используем SIGKILL..."
        kill -KILL $PID 2>/dev/null
        sleep 0.5
    else
        echo "✅ Процесс $PID успешно остановлен"
    fi
done

echo ""
echo "✅ Готово! Все процессы остановлены"

