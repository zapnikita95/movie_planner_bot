#!/usr/bin/env python3
"""
Скрипт для удаления файла top_actors.txt на Railway
Запускается через переменную окружения DELETE_TOP_ACTORS_FILE=1
"""
import os
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Путь к файлу
DATA_DIR = Path('data/shazam')
TOP_ACTORS_PATH = DATA_DIR / 'top_actors.txt'

def delete_top_actors_file():
    """Удаляет файл top_actors.txt если он существует"""
    if TOP_ACTORS_PATH.exists():
        try:
            file_size = TOP_ACTORS_PATH.stat().st_size
            TOP_ACTORS_PATH.unlink()
            logger.info(f"✅ Файл top_actors.txt удалён (размер был: {file_size} байт)")
            logger.info(f"   Путь: {TOP_ACTORS_PATH.absolute()}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка удаления файла: {e}", exc_info=True)
            return False
    else:
        logger.info(f"ℹ️ Файл top_actors.txt не существует: {TOP_ACTORS_PATH.absolute()}")
        return False

if __name__ == "__main__":
    # Проверяем переменную окружения
    if os.getenv('DELETE_TOP_ACTORS_FILE', '0').strip().lower() in ('1', 'true', 'yes', 'on'):
        logger.info("DELETE_TOP_ACTORS_FILE=1 - удаляем файл top_actors.txt...")
        delete_top_actors_file()
    else:
        logger.info("DELETE_TOP_ACTORS_FILE не установлен - файл не удаляется")
