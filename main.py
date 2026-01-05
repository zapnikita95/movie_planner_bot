"""
Главная точка входа приложения
Использует новую структуру из moviebot/

⚠️ ВНИМАНИЕ: Этот файл в корне проекта - это обертка.
Реальная точка входа находится в moviebot/main.py
Используйте: python -m moviebot.main
"""
import sys
import logging

logger = logging.getLogger(__name__)

# Проверяем, что не пытаемся запустить старый файл
if 'moviebot.py' in sys.argv[0] or 'OLD_DO_NOT_USE' in sys.argv[0]:
    logger.error("❌ ОШИБКА: Попытка запуска старого файла!")
    logger.error("✅ Используйте: python -m moviebot.main")
    sys.exit(1)

# Импортируем и запускаем из новой структуры
if __name__ == '__main__':
    logger.info("✅ Используется правильная точка входа через moviebot.main")
    from moviebot.main import *
    # Код из moviebot/main.py уже выполнится при импорте
