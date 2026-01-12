"""
Сервис для работы с API nalog.ru (самозанятый) — рабочая версия на январь 2026
"""
import requests
import base64
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Кэш токена (на 23 часа)
_token_cache: Optional[str] = None
_token_expires_at: Optional[datetime] = None


def get_nalog_token() -> Optional[str]:
    """Получает и кэширует токен"""
    global _token_cache, _token_expires_at

    if _token_cache and _token_expires_at and datetime.now() < _token_expires_at:
        logger.info("[NALOG] Токен из кэша")
        return _token_cache

    inn = os.getenv('NALOG_INN', '').strip()
    password = os.getenv('NALOG_PASSWORD', '').strip()

    if not inn or not password:
        logger.error("[NALOG] ❌ NALOG_INN или NALOG_PASSWORD не заданы/пустые")
        return None

    try:
        auth_str = base64.b64encode(f"{inn}:{password}".encode()).decode()
        headers = {"Authorization": f"Basic {auth_str}", "Content-Type": "application/json"}

        logger.info("[NALOG] Запрос токена...")
        resp = requests.post("https://lknpd.nalog.ru/api/v1/auth", headers=headers, timeout=10)

        if resp.status_code == 200:
            token = resp.json().get('token')
            if token:
                _token_cache = token
                _token_expires_at = datetime.now() + timedelta(hours=23)
                logger.info("[NALOG] ✅ Токен получен")
                return token
            logger.error(f"[NALOG] Токен не в ответе: {resp.json()}")
        else:
            logger.error(f"[NALOG] Ошибка токена: {resp.status_code} {resp.text}")

        return None
    except Exception as e:
        logger.error(f"[NALOG] Ошибка получения токена: {e}", exc_info=True)
        return None


def create_check(amount_rub: float, description: str, user_name: Optional[str] = None) -> Tuple[Optional[str], Optional[str]]:
    """Создаёт чек и возвращает ссылки на просмотр + PDF"""
    try:
        token = get_nalog_token()
        if not token:
            logger.error("[NALOG] ❌ Нет токена")
            return None, None

        inn = os.getenv('NALOG_INN', '').strip()
        if not inn:
            logger.error("[NALOG] ❌ NALOG_INN не задан")
            return None, None

        now_iso = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'

        payload = {
            "requestTime": now_iso,
            "operationTime": now_iso,
            "services": [{
                "name": description,
                "amount": float(amount_rub),
                "quantity": 1
            }],
            "totalAmount": float(amount_rub),
            "paymentType": "ELECTRONIC",  # Для онлайн-платежей (YooKassa)
            "client": {
                "displayName": user_name or "Пользователь Telegram",
                "incomeType": "FROM_INDIVIDUAL"
            },
            "ignoreMaxTotalIncomeRestriction": False
        }

        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        logger.info(f"[NALOG] ===== СОЗДАНИЕ ЧЕКА ===== amount={amount_rub} desc=\"{description}\"")
        logger.info(f"[NALOG] Payload: {payload}")

        resp = requests.post("https://lknpd.nalog.ru/api/v1/income", json=payload, headers=headers, timeout=15)

        if resp.status_code == 200:
            data = resp.json()
            uuid = data.get('approvedReceiptUuid') or data.get('id')
            if uuid:
                check_url = f"https://lknpd.nalog.ru/receipt/{inn}/{uuid}"
                pdf_url = f"https://lknpd.nalog.ru/api/v1/receipt/{inn}/{uuid}/print"

                logger.info("[NALOG] ✅ ЧЕК СОЗДАН!")
                logger.info(f"[NALOG] check_url={check_url}")
                logger.info(f"[NALOG] pdf_url={pdf_url}")
                return check_url, pdf_url

            logger.error(f"[NALOG] UUID не найден в ответе: {data}")
        else:
            logger.error(f"[NALOG] Ошибка API: {resp.status_code} {resp.text}")

        return None, None
    except Exception as e:
        logger.error(f"[NALOG] Исключение: {e}", exc_info=True)
        return None, None
