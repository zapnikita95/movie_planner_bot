"""
Watchdog модуль для мониторинга критических компонентов бота
Обеспечивает автоматический перезапуск при сбоях
"""
import logging
import time
import threading
from datetime import datetime
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class BotWatchdog:
    """Watchdog для мониторинга состояния бота и его компонентов"""
    
    def __init__(self, check_interval: int = 60):
        """
        Args:
            check_interval: Интервал проверки в секундах (по умолчанию 60)
        """
        self.check_interval = check_interval
        self.running = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.last_crash_time: Optional[datetime] = None
        self.crash_count = 0
        self.last_check_time: Optional[datetime] = None
        self.scheduler_instance = None
        self.db_connection = None
        self.bot_instance = None
        self.health_status: Dict[str, Any] = {
            'scheduler': {'status': 'unknown', 'last_check': None, 'error': None},
            'database': {'status': 'unknown', 'last_check': None, 'error': None},
            'bot': {'status': 'unknown', 'last_check': None, 'error': None}
        }
        
    def register_scheduler(self, scheduler):
        """Регистрирует scheduler для мониторинга"""
        self.scheduler_instance = scheduler
        logger.info("[WATCHDOG] Scheduler зарегистрирован для мониторинга")
        
#     def register_database(self, db_connection):
#        """Регистрирует подключение к БД для мониторинга"""
#        self.db_connection = db_connection
#        logger.info("[WATCHDOG] База данных зарегистрирована для мониторинга")
        
    def register_bot(self, bot):
        """Регистрирует бота для мониторинга"""
        self.bot_instance = bot
        logger.info("[WATCHDOG] Бот зарегистрирован для мониторинга")
        
    def check_scheduler(self) -> bool:
        """Проверяет состояние scheduler"""
        try:
            if self.scheduler_instance is None:
                self.health_status['scheduler'] = {
                    'status': 'not_registered',
                    'last_check': datetime.now().isoformat(),
                    'error': 'Scheduler не зарегистрирован'
                }
                return False
                
            # Проверяем, запущен ли scheduler
            if not self.scheduler_instance.running:
                self.health_status['scheduler'] = {
                    'status': 'stopped',
                    'last_check': datetime.now().isoformat(),
                    'error': 'Scheduler остановлен'
                }
                logger.error("[WATCHDOG] ❌ Scheduler остановлен!")
                return False
                
            # Проверяем количество активных задач
            jobs = self.scheduler_instance.get_jobs()
            self.health_status['scheduler'] = {
                'status': 'running',
                'last_check': datetime.now().isoformat(),
                'error': None,
                'jobs_count': len(jobs)
            }
            return True
            
        except Exception as e:
            error_msg = str(e)
            self.health_status['scheduler'] = {
                'status': 'error',
                'last_check': datetime.now().isoformat(),
                'error': error_msg
            }
            logger.error(f"[WATCHDOG] ❌ Ошибка проверки scheduler: {e}", exc_info=True)
            return False
            
def check_database(self) -> bool:
    """Проверяет состояние подключения к БД с retry"""
    from moviebot.database.db_connection import get_db_connection
    
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            conn = get_db_connection()
            try:
                conn.rollback()
            except:
                pass
            
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            finally:
                cursor.close()
            
            self.health_status['database'] = {
                'status': 'healthy',
                'last_check': datetime.now().isoformat(),
                'error': None,
                'attempt': attempt
            }
            logger.debug("[WATCHDOG] БД проверена успешно")
            return True
        
        except Exception as e:
            logger.warning(f"[WATCHDOG] Проблема с БД (попытка {attempt}/{max_retries}): {e}")
            time.sleep(1)  # пауза перед повтором
    
    self.health_status['database'] = {
        'status': 'unhealthy',
        'last_check': datetime.now().isoformat(),
        'error': f"Не удалось после {max_retries} попыток"
    }
    return False
            
    def check_bot(self) -> bool:
        """Проверяет состояние бота"""
        try:
            if self.bot_instance is None:
                self.health_status['bot'] = {
                    'status': 'not_registered',
                    'last_check': datetime.now().isoformat(),
                    'error': 'Бот не зарегистрирован'
                }
                return False
                
            # Пытаемся получить информацию о боте
            bot_info = self.bot_instance.get_me()
            if bot_info is None:
                raise Exception("Не удалось получить информацию о боте")
                
            self.health_status['bot'] = {
                'status': 'running',
                'last_check': datetime.now().isoformat(),
                'error': None,
                'bot_id': bot_info.id,
                'bot_username': bot_info.username
            }
            return True
            
        except Exception as e:
            error_msg = str(e)
            self.health_status['bot'] = {
                'status': 'error',
                'last_check': datetime.now().isoformat(),
                'error': error_msg
            }
            logger.error(f"[WATCHDOG] ❌ Ошибка проверки бота: {e}", exc_info=True)
            return False
            
    def check_all(self) -> Dict[str, bool]:
        """Проверяет все компоненты"""
        results = {
            'scheduler': self.check_scheduler(),
            'database': self.check_database(),
            'bot': self.check_bot()
        }
        self.last_check_time = datetime.now()
        return results
        
    def _monitor_loop(self):
        """Основной цикл мониторинга"""
        logger.info(f"[WATCHDOG] 🐕 Watchdog запущен (интервал проверки: {self.check_interval} сек)")
        
        while self.running:
            try:
                time.sleep(self.check_interval)
                
                if not self.running:
                    break
                    
                logger.debug("[WATCHDOG] Проверка компонентов...")
                results = self.check_all()
                
                # Логируем результаты
                all_ok = all(results.values())
                if all_ok:
                    logger.debug("[WATCHDOG] ✅ Все компоненты работают нормально")
                else:
                    failed = [name for name, status in results.items() if not status]
                    logger.warning(f"[WATCHDOG] ⚠️ Проблемы обнаружены: {', '.join(failed)}")
                    
            except Exception as e:
                logger.error(f"[WATCHDOG] ❌ Ошибка в цикле мониторинга: {e}", exc_info=True)
                time.sleep(5)  # Короткая пауза перед следующей попыткой
                
        logger.info("[WATCHDOG] 🐕 Watchdog остановлен")
        
    def start(self):
        """Запускает watchdog"""
        if self.running:
            logger.warning("[WATCHDOG] Watchdog уже запущен")
            return
            
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        logger.info("[WATCHDOG] ✅ Watchdog запущен")
        
    def stop(self):
        """Останавливает watchdog"""
        if not self.running:
            return
            
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        logger.info("[WATCHDOG] ✅ Watchdog остановлен")
        
    def record_crash(self, error: Exception):
        """Записывает информацию о краше"""
        self.last_crash_time = datetime.now()
        self.crash_count += 1
        logger.critical(
            f"[WATCHDOG] 💥 КРАШ ЗАФИКСИРОВАН! "
            f"Время: {self.last_crash_time.isoformat()}, "
            f"Всего крашей: {self.crash_count}, "
            f"Ошибка: {error}"
        )
        
    def get_health_status(self) -> Dict[str, Any]:
        """Возвращает текущий статус здоровья всех компонентов"""
        return {
            'overall': 'healthy' if all(
                status.get('status') in ('running', 'connected', 'unknown') 
                for status in self.health_status.values()
            ) else 'unhealthy',
            'components': self.health_status.copy(),
            'last_check': self.last_check_time.isoformat() if self.last_check_time else None,
            'last_crash': self.last_crash_time.isoformat() if self.last_crash_time else None,
            'crash_count': self.crash_count
        }

# Глобальный экземпляр watchdog
_watchdog_instance: Optional[BotWatchdog] = None

def get_watchdog(check_interval: int = 60) -> BotWatchdog:
    """Получает или создает глобальный экземпляр watchdog"""
    global _watchdog_instance
    if _watchdog_instance is None:
        _watchdog_instance = BotWatchdog(check_interval=check_interval)
    return _watchdog_instance








