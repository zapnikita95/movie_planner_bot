#!/usr/bin/env python3
"""
Скрипт для поиска и остановки старых экземпляров бота
Проверяет все процессы Python, которые могут быть старыми экземплярами бота
"""
import subprocess
import sys
import os
import signal
import time

def get_python_processes():
    """Получает список всех процессов Python"""
    try:
        result = subprocess.run(['ps', 'aux'], capture_output=True, text=True)
        lines = result.stdout.split('\n')
        python_processes = []
        for line in lines:
            if 'python' in line.lower() and 'kill_old_bots' not in line:
                parts = line.split()
                if len(parts) >= 2:
                    pid = parts[1]
                    cmd = ' '.join(parts[10:])  # Команда начинается с 11-го элемента
                    python_processes.append((pid, cmd))
        return python_processes
    except Exception as e:
        print(f"Ошибка при получении процессов: {e}")
        return []

def find_bot_processes():
    """Находит процессы, связанные с ботом"""
    processes = get_python_processes()
    bot_processes = []
    
    keywords = [
        'moviebot',
        'movie_planner_bot',
        'main.py',
        'moviebot.py',
        'bot.polling',
        'telebot',
        'python -m moviebot.main',
        'python moviebot/main.py',
        'python main.py'
    ]
    
    for pid, cmd in processes:
        cmd_lower = cmd.lower()
        for keyword in keywords:
            if keyword.lower() in cmd_lower:
                # Проверяем, что это не текущий скрипт
                if 'kill_old_bots' not in cmd_lower:
                    bot_processes.append((pid, cmd))
                    break
    
    return bot_processes

def kill_process(pid):
    """Останавливает процесс по PID"""
    try:
        pid_int = int(pid)
        os.kill(pid_int, signal.SIGTERM)
        time.sleep(1)
        # Проверяем, завершился ли процесс
        try:
            os.kill(pid_int, 0)  # Проверка существования процесса
            # Если процесс все еще существует, используем SIGKILL
            print(f"Процесс {pid} не завершился, используем SIGKILL...")
            os.kill(pid_int, signal.SIGKILL)
            time.sleep(0.5)
        except ProcessLookupError:
            print(f"✅ Процесс {pid} успешно остановлен")
            return True
    except Exception as e:
        print(f"❌ Ошибка при остановке процесса {pid}: {e}")
        return False
    return True

def main():
    print("🔍 Поиск процессов бота...")
    bot_processes = find_bot_processes()
    
    if not bot_processes:
        print("✅ Старые процессы бота не найдены")
        return
    
    print(f"\n📋 Найдено {len(bot_processes)} процесс(ов) бота:")
    for pid, cmd in bot_processes:
        print(f"  PID: {pid}")
        print(f"  Команда: {cmd[:100]}...")
        print()
    
    response = input("Остановить все найденные процессы? (yes/no): ")
    if response.lower() in ['yes', 'y', 'да', 'д']:
        print("\n🛑 Остановка процессов...")
        for pid, cmd in bot_processes:
            print(f"Останавливаю PID {pid}...")
            kill_process(pid)
        print("\n✅ Готово! Все процессы остановлены")
    else:
        print("Отменено")

if __name__ == '__main__':
    main()

