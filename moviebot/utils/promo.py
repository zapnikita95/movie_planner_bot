"""
Модуль для работы с промокодами
"""
import logging
import re
from decimal import Decimal, ROUND_DOWN
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


def create_promocode(code, discount_input, total_uses):
    """
    Создает промокод
    
    Args:
        code: Код промокода (без пробелов)
        discount_input: Скидка в формате "20%" или "30" (фиксированная сумма)
        total_uses: Количество использований
    
    Returns:
        (success: bool, message: str)
    """
    try:
        # Парсим скидку
        discount_type = None
        discount_value = None
        
        if '%' in discount_input:
            # Процентная скидка
            discount_type = 'percent'
            discount_value = float(re.sub(r'%', '', discount_input.strip()))
            if not (0 < discount_value <= 100):
                return False, "Процентная скидка должна быть от 1% до 100%"
        else:
            # Фиксированная скидка
            discount_type = 'fixed'
            try:
                discount_value = float(discount_input.strip())
                if discount_value <= 0:
                    return False, "Фиксированная скидка должна быть больше 0"
            except ValueError:
                return False, "Неверный формат скидки"
        
        # Проверяем количество использований
        try:
            total_uses = int(total_uses)
            if total_uses <= 0:
                return False, "Количество использований должно быть больше 0"
        except ValueError:
            return False, "Количество использований должно быть целым числом"
        
        # Проверяем, не существует ли уже такой промокод
        with db_lock:
            cursor.execute('SELECT id FROM promocodes WHERE code = %s', (code.upper(),))
            existing = cursor.fetchone()
            if existing:
                return False, f"Промокод {code.upper()} уже существует"
            
            # Создаем промокод
            cursor.execute('''
                INSERT INTO promocodes (code, discount_type, discount_value, total_uses, is_active)
                VALUES (%s, %s, %s, %s, TRUE)
            ''', (code.upper(), discount_type, discount_value, total_uses))
            conn.commit()
        
        discount_str = f"{discount_value}%" if discount_type == 'percent' else f"{int(discount_value)} руб/звезд"
        return True, f"Промокод задан: {discount_str} {total_uses}"
        
    except Exception as e:
        logger.error(f"Ошибка при создании промокода: {e}", exc_info=True)
        conn.rollback()
        return False, f"Ошибка при создании промокода: {e}"


def get_active_promocodes():
    """
    Получает список активных промокодов
    
    Returns:
        list of dict: [{'id': int, 'code': str, 'discount_type': str, 'discount_value': float, 
                       'total_uses': int, 'used_count': int}]
    """
    try:
        with db_lock:
            cursor.execute('''
                SELECT id, code, discount_type, discount_value, total_uses, used_count
                FROM promocodes
                WHERE is_active = TRUE AND used_count < total_uses
                ORDER BY created_at DESC
            ''')
            rows = cursor.fetchall()
            
            result = []
            for row in rows:
                if isinstance(row, dict):
                    result.append({
                        'id': row['id'],
                        'code': row['code'],
                        'discount_type': row['discount_type'],
                        'discount_value': float(row['discount_value']),
                        'total_uses': row['total_uses'],
                        'used_count': row['used_count']
                    })
                else:
                    result.append({
                        'id': row[0],
                        'code': row[1],
                        'discount_type': row[2],
                        'discount_value': float(row[3]),
                        'total_uses': row[4],
                        'used_count': row[5]
                    })
            return result
    except Exception as e:
        logger.error(f"Ошибка при получении активных промокодов: {e}", exc_info=True)
        return []


def get_promocode_info(code):
    """
    Получает информацию о промокоде
    
    Args:
        code: Код промокода
    
    Returns:
        dict or None: {'id': int, 'code': str, 'discount_type': str, 'discount_value': float,
                      'total_uses': int, 'used_count': int, 'is_active': bool}
    """
    try:
        with db_lock:
            cursor.execute('''
                SELECT id, code, discount_type, discount_value, total_uses, used_count, is_active
                FROM promocodes
                WHERE code = %s
            ''', (code.upper(),))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            if isinstance(row, dict):
                return {
                    'id': row['id'],
                    'code': row['code'],
                    'discount_type': row['discount_type'],
                    'discount_value': float(row['discount_value']),
                    'total_uses': row['total_uses'],
                    'used_count': row['used_count'],
                    'is_active': bool(row['is_active'])
                }
            else:
                return {
                    'id': row[0],
                    'code': row[1],
                    'discount_type': row[2],
                    'discount_value': float(row[3]),
                    'total_uses': row[4],
                    'used_count': row[5],
                    'is_active': bool(row[6])
                }
    except Exception as e:
        logger.error(f"Ошибка при получении информации о промокоде: {e}", exc_info=True)
        return None


def apply_promocode(code, original_amount, user_id, chat_id):
    """
    Применяет промокод к сумме
    
    Args:
        code: Код промокода
        original_amount: Исходная сумма (в рублях или звездах)
        user_id: ID пользователя
        chat_id: ID чата
    
    Returns:
        (success: bool, discounted_amount: float, message: str, promocode_id: int or None)
    """
    try:
        # Приводим код к верхнему регистру для сравнения
        code_upper = code.upper().strip()
        logger.info(f"[APPLY PROMOCODE] Применяем промокод: code='{code}', code_upper='{code_upper}', original_amount={original_amount}, user_id={user_id}, chat_id={chat_id}")
        
        promocode_info = get_promocode_info(code_upper)
        
        if not promocode_info:
            logger.warning(f"[APPLY PROMOCODE] Промокод не найден: code_upper='{code_upper}'")
            return False, original_amount, "Промокод не найден", None
        
        if not promocode_info['is_active']:
            return False, original_amount, "Промокод деактивирован", None
        
        if promocode_info['used_count'] >= promocode_info['total_uses']:
            return False, original_amount, "Лимит использований промокода исчерпан", None
        
        # Применяем скидку
        discount_type = promocode_info['discount_type']
        discount_value = promocode_info['discount_value']
        
        if discount_type == 'percent':
            # Процентная скидка
            discount_amount = original_amount * (discount_value / 100)
            discounted_amount = original_amount - discount_amount
        else:
            # Фиксированная скидка
            discounted_amount = original_amount - discount_value
            if discounted_amount < 0:
                discounted_amount = 0
        
        # Округляем вниз до целого числа
        discounted_amount = float(Decimal(str(discounted_amount)).quantize(Decimal('1'), rounding=ROUND_DOWN))
        
        # Увеличиваем счетчик использований
        with db_lock:
            cursor.execute('''
                UPDATE promocodes
                SET used_count = used_count + 1
                WHERE id = %s
            ''', (promocode_info['id'],))
            
            # Записываем использование
            cursor.execute('''
                INSERT INTO promocode_uses (promocode_id, user_id, chat_id)
                VALUES (%s, %s, %s)
            ''', (promocode_info['id'], user_id, chat_id))
            
            conn.commit()
        
        discount_str = f"{discount_value}%" if discount_type == 'percent' else f"{int(discount_value)} руб/звезд"
        return True, discounted_amount, f"Промокод применен! Скидка: {discount_str}", promocode_info['id']
        
    except Exception as e:
        logger.error(f"Ошибка при применении промокода: {e}", exc_info=True)
        conn.rollback()
        return False, original_amount, f"Ошибка при применении промокода: {e}", None


def deactivate_promocode(promocode_id):
    """
    Деактивирует промокод
    
    Args:
        promocode_id: ID промокода
    
    Returns:
        (success: bool, message: str)
    """
    try:
        with db_lock:
            cursor.execute('''
                UPDATE promocodes
                SET is_active = FALSE, deactivated_at = NOW()
                WHERE id = %s
            ''', (promocode_id,))
            conn.commit()
        
        return True, "Промокод деактивирован"
    except Exception as e:
        logger.error(f"Ошибка при деактивации промокода: {e}", exc_info=True)
        conn.rollback()
        return False, f"Ошибка при деактивации промокода: {e}"


def get_promocode_statistics():
    """
    Получает статистику по промокодам
    
    Returns:
        dict: {'total_promocodes': int, 'active_promocodes': int, 
               'total_uses': int, 'promocodes': list}
    """
    try:
        with db_lock:
            # Общая статистика
            cursor.execute('SELECT COUNT(*) FROM promocodes')
            total_promocodes = cursor.fetchone()
            total_promocodes = total_promocodes.get('count') if isinstance(total_promocodes, dict) else total_promocodes[0]
            
            cursor.execute('SELECT COUNT(*) FROM promocodes WHERE is_active = TRUE')
            active_promocodes = cursor.fetchone()
            active_promocodes = active_promocodes.get('count') if isinstance(active_promocodes, dict) else active_promocodes[0]
            
            cursor.execute('SELECT SUM(used_count) FROM promocodes')
            total_uses = cursor.fetchone()
            total_uses = total_uses.get('sum') if total_uses and total_uses.get('sum') else 0
            if total_uses is None:
                total_uses = 0
            
            # Детальная статистика по промокодам
            cursor.execute('''
                SELECT code, discount_type, discount_value, total_uses, used_count, is_active
                FROM promocodes
                ORDER BY created_at DESC
            ''')
            rows = cursor.fetchall()
            
            promocodes_list = []
            for row in rows:
                if isinstance(row, dict):
                    promocodes_list.append({
                        'code': row['code'],
                        'discount_type': row['discount_type'],
                        'discount_value': float(row['discount_value']),
                        'total_uses': row['total_uses'],
                        'used_count': row['used_count'],
                        'is_active': bool(row['is_active']),
                        'remaining': row['total_uses'] - row['used_count']
                    })
                else:
                    promocodes_list.append({
                        'code': row[0],
                        'discount_type': row[1],
                        'discount_value': float(row[2]),
                        'total_uses': row[3],
                        'used_count': row[4],
                        'is_active': bool(row[5]),
                        'remaining': row[3] - row[4]
                    })
            
            return {
                'total_promocodes': total_promocodes,
                'active_promocodes': active_promocodes,
                'total_uses': int(total_uses),
                'promocodes': promocodes_list
            }
    except Exception as e:
        logger.error(f"Ошибка при получении статистики промокодов: {e}", exc_info=True)
        return {
            'total_promocodes': 0,
            'active_promocodes': 0,
            'total_uses': 0,
            'promocodes': []
        }

