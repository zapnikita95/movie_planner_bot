"""
Сервис для работы с API nalog.ru (самозанятый)
"""
import requests
import base64
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Кэш токена
_token_cache = None
_token_expires_at = None


def get_nalog_token() -> Optional[str]:
    """
    Получает токен авторизации от nalog.ru API
    Токен кэшируется на 24 часа
    """
    global _token_cache, _token_expires_at

    if _token_cache and _token_expires_at and datetime.now() < _token_expires_at:
        logger.info("[NALOG] Используем токен из кэша")
        return _token_cache

    INN = os.getenv('NALOG_INN')
    PASSWORD = os.getenv('NALOG_PASSWORD')

    logger.info(f"[NALOG] NALOG_INN присутствует: {bool(INN)}, значение: {'***' if INN else 'None'}")
    logger.info(f"[NALOG] NALOG_PASSWORD присутствует: {bool(PASSWORD)}, значение: {'***' if PASSWORD else 'None'}")

    if INN:
        INN = INN.strip()
    if PASSWORD:
        PASSWORD = PASSWORD.strip()

    if not INN or not PASSWORD:
        logger.error("[NALOG] ❌ NALOG_INN или NALOG_PASSWORD не заданы!")
        return None

    try:
        auth_str = base64.b64encode(f"{INN}:{PASSWORD}".encode()).decode()
        headers = {
            "Authorization": f"Basic {auth_str}",
            "Content-Type": "application/json"
        }

        logger.info("[NALOG] Запрос токена...")
        resp = requests.post("https://lknpd.nalog.ru/api/v1/auth", headers=headers, timeout=10)

        if resp.status_code == 200:
            data = resp.json()
            token = data.get('token')
            if token:
                _token_cache = token
                _token_expires_at = datetime.now() + timedelta(hours=24)
                logger.info("[NALOG] ✅ Токен получен и закэширован")
                return token
            else:
                logger.error(f"[NALOG] Токен не найден в ответе: {data}")
        else:
            logger.error(f"[NALOG] Ошибка токена: {resp.status_code} - {resp.text}")

        return None

    except Exception as e:
        logger.error(f"[NALOG] Исключение при получении токена: {e}", exc_info=True)
        return None


def create_check(amount_rub: float, description: str, user_name: Optional[str] = None, user_inn: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    """
    Создает чек через API nalog.ru
    """
    try:
        token = get_nalog_token()
        if not token:
            logger.error("[NALOG] Не удалось получить токен")
            return None, None

        now_iso = datetime.utcnow().isoformat() + "Z"  # UTC для API

        payload = {
            "paymentType": "BANK_PAYMENT",
            "requestTime": now_iso,
            "operationTime": now_iso,
            "services": [
                {
                    "name": description,
                    "amount": float(amount_rub),
                    "quantity": 1
                }
            ],
            "totalAmount": float(amount_rub),
            "client": {
                "displayName": user_name or "Анонимный пользователь",
                "incomeType": "FROM_INDIVIDUAL"
            },
            "ignoreMaxTotalIncomeRestriction": False
        }

        # Если user_inn всё же нужен (редко), можно добавить:
        # if user_inn:
        #     payload["client"]["inn"] = user_inn
        #     payload["client"]["incomeType"] = "FROM_LEGAL_ENTITY"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        logger.info(f"[NALOG] Создание чека | amount={amount_rub} | desc={description}")
        logger.info(f"[NALOG] Payload: {payload}")

        resp = requests.post(
            "https://lknpd.nalog.ru/api/v1/income",
            json=payload,
            headers=headers,
            timeout=15
        )

        if resp.status_code == 200:
            data = resp.json()
            check_url = data.get('receiptUrl') or data.get('checkUrl')
            pdf_url = data.get('pdfUrl') or data.get('printUrl')

            if check_url:
                logger.info(f"[NALOG] ✅ Чек создан! check_url={check_url}, pdf_url={pdf_url}")
                return check_url, pdf_url
            else:
                logger.error(f"[NALOG] Нет ссылок в ответе: {data}")
        else:
            logger.error(f"[NALOG] Ошибка создания чека: {resp.status_code} → {resp.text}")

        return None, None

    except Exception as e:
        logger.error(f"[NALOG] Исключение при создании чека: {e}", exc_info=True)
        return None, None