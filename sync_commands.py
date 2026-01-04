#!/usr/bin/env python3
"""
Утилита для ручной синхронизации команд бота с Telegram API.
Можно запустить отдельно, если нужно обновить команды вручную.
"""
import sys
import os

# Добавляем путь к проекту
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bot.bot_init import setup_bot_commands, bot, logger

def main():
    """Ручная синхронизация команд"""
    print("=" * 60)
    print("СИНХРОНИЗАЦИЯ КОМАНД БОТА С TELEGRAM API")
    print("=" * 60)
    print()
    
    try:
        # Проверяем, что бот доступен
        bot_info = bot.get_me()
        print(f"✓ Бот подключен: @{bot_info.username} (ID: {bot_info.id})")
        print()
        
        # Синхронизируем команды
        print("Синхронизация команд...")
        success = setup_bot_commands(bot)
        
        print()
        if success:
            print("✅ Команды успешно синхронизированы для всех scope!")
        else:
            print("⚠️  Команды синхронизированы частично. Проверьте логи выше.")
        
        print()
        print("=" * 60)
        
    except Exception as e:
        logger.error(f"Ошибка при синхронизации команд: {e}", exc_info=True)
        print(f"❌ Ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

