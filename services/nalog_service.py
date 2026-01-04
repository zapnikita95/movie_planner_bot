"""
Сервис для работы с API nalog.ru (самозанятый)
"""
import requests
import base64
import logging
import os
from datetime import datetime
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Кэш для токена (чтобы не запрашивать каждый раз)
_token_cache = None
_token_expires_at = None

def get_nalog_token() -> Optional[str]:
    """
    Получает токен авторизации от nalog.ru API
    Токен кэшируется, чтобы не запрашивать каждый раз
    """
    global _token_cache, _token_expires_at
    
    # Проверяем, есть ли валидный токен в кэше
    if _token_cache and _token_expires_at:
        if datetime.now() < _token_expires_at:
            logger.info("[NALOG] Используем токен из кэша")
            return _token_cache
    
    INN = os.getenv('NALOG_INN')
    PASSWORD = os.getenv('NALOG_PASSWORD')
    
    # Детальное логирование для отладки
    logger.info(f"[NALOG] Проверка переменных окружения:")
    logger.info(f"[NALOG] NALOG_INN присутствует: {INN is not None}, значение: {'***' if INN else 'None'}")
    logger.info(f"[NALOG] NALOG_PASSWORD присутствует: {PASSWORD is not None}, значение: {'***' if PASSWORD else 'None'}")
    
    # Проверяем, что значения не пустые (после strip)
    if INN:
        INN = INN.strip()
    if PASSWORD:
        PASSWORD = PASSWORD.strip()
    
    if not INN or not PASSWORD:
        logger.error("[NALOG] ❌ NALOG_INN или NALOG_PASSWORD не заданы в переменных окружения!")
        logger.error(f"[NALOG] NALOG_INN: {'установлен (пусто после strip)' if INN is not None and not INN else 'НЕ УСТАНОВЛЕН'}")
        logger.error(f"[NALOG] NALOG_PASSWORD: {'установлен (пусто после strip)' if PASSWORD is not None and not PASSWORD else 'НЕ УСТАНОВЛЕН'}")
        return None
    
    try:
        auth_str = base64.b64encode(f"{INN}:{PASSWORD}".encode()).decode()
        headers = {
            "Authorization": f"Basic {auth_str}",
            "Content-Type": "application/json"
        }
        
        logger.info("[NALOG] Запрос токена авторизации...")
        token_resp = requests.post("https://lknpd.nalog.ru/api/v1/auth", headers=headers, timeout=10)
        
        if token_resp.status_code == 200:
            token_data = token_resp.json()
            token = token_data.get('token')
            
            if token:
                # Сохраняем токен в кэш (считаем, что он валиден 24 часа)
                _token_cache = token
                _token_expires_at = datetime.now().replace(hour=23, minute=59, second=59)
                logger.info("[NALOG] ✅ Токен успешно получен и сохранен в кэш")
                return token
            else:
                logger.error(f"[NALOG] ❌ Токен не найден в ответе: {token_data}")
                return None
        else:
            logger.error(f"[NALOG] ❌ Ошибка получения токена: {token_resp.status_code} - {token_resp.text}")
            return None
            
    except Exception as e:
        logger.error(f"[NALOG] ❌ Исключение при получении токена: {e}", exc_info=True)
        return None


def create_check(amount_rub: float, description: str, user_name: Optional[str] = None, user_inn: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    """
    Создает чек от самозанятого через API nalog.ru
    
    Args:
        amount_rub: Сумма в рублях (с копейками, например 299.00)
        description: Описание услуги (например, "Премиум-подписка MovieBot на месяц")
        user_name: Имя пользователя (опционально)
        user_inn: ИНН пользователя (опционально)
    
    Returns:
        Tuple[check_url, pdf_url] или (None, None) в случае ошибки
    """
    try:
        token = get_nalog_token()
        if not token:
            logger.error("[NALOG] ❌ Не удалось получить токен для создания чека")
            return None, None
        
        # Формируем payload
        client_data = {
            "name": user_name or "Пользователь Telegram"
        }
        if user_inn:
            client_data["inn"] = user_inn
        
        payload = {
            "requestTime": datetime.now().isoformat(),
            "operation": "INCOME",
            "income": {
                "amount": float(amount_rub),  # В рублях, с копейками
                "description": description,
                "quantity": 1,
                "client": client_data
            }
        }
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        logger.info(f"[NALOG] ===== СОЗДАНИЕ ЧЕКА =====")
        logger.info(f"[NALOG] amount={amount_rub}, description={description}, user_name={user_name}")
        logger.info(f"[NALOG] Отправка запроса на создание чека...")
        
        resp = requests.post(
            "https://lknpd.nalog.ru/api/v1/income",
            json=payload,
            headers=headers,
            timeout=10
        )
        
        if resp.status_code == 200:
            check_data = resp.json()
            check_url = check_data.get('checkUrl')  # Ссылка на чек
            pdf_url = check_data.get('pdfUrl')      # Прямая ссылка на PDF
            
            logger.info(f"[NALOG] ✅✅✅ ЧЕК УСПЕШНО СОЗДАН! ✅✅✅")
            logger.info(f"[NALOG] check_url={check_url}")
            logger.info(f"[NALOG] pdf_url={pdf_url}")
            logger.info(f"[NALOG] ===== СОЗДАНИЕ ЧЕКА ЗАВЕРШЕНО =====")
            
            return check_url, pdf_url
        else:
            logger.error(f"[NALOG] ❌ Ошибка создания чека: {resp.status_code} - {resp.text}")
            return None, None
            
    except Exception as e:
        logger.error(f"[NALOG] ❌ Исключение при создании чека: {e}", exc_info=True)
        return None, None

