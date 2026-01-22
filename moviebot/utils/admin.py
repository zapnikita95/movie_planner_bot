"""
Утилиты для работы с администраторами
"""
import logging
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock

logger = logging.getLogger(__name__)


def is_admin(user_id):
    """
    Проверяет, является ли пользователь администратором
    
    Args:
        user_id: ID пользователя
    
    Returns:
        bool: True если пользователь администратор
    """
    conn_local = get_db_connection()
    cursor_local = None
    
    try:
        cursor_local = conn_local.cursor()
        with db_lock:
            cursor_local.execute('SELECT id FROM admins WHERE user_id = %s AND is_active = TRUE', (user_id,))
            row = cursor_local.fetchone()
            return row is not None
    except Exception as e:
        logger.error(f"Ошибка при проверке прав администратора: {e}", exc_info=True)
        return False
    finally:
        if cursor_local:
            try:
                cursor_local.close()
            except:
                pass
        try:
            conn_local.close()
        except:
            pass


def is_owner(user_id):
    """
    Проверяет, является ли пользователь владельцем бота
    
    Args:
        user_id: ID пользователя
    
    Returns:
        bool: True если пользователь владелец
    """
    return user_id == 301810276


def add_admin(user_id, added_by):
    """
    Добавляет администратора
    
    Args:
        user_id: ID нового администратора
        added_by: ID пользователя, который добавляет администратора
    
    Returns:
        (success: bool, message: str)
    """
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        # Проверяем, не является ли уже администратором
        with db_lock:
            cursor_local.execute('SELECT id, is_active FROM admins WHERE user_id = %s', (user_id,))
            row = cursor_local.fetchone()
            
            if row:
                admin_id = row.get('id') if isinstance(row, dict) else row[0]
                is_active = row.get('is_active') if isinstance(row, dict) else row[1]
                
                if is_active:
                    return False, "Пользователь уже является администратором"
                else:
                    # Активируем существующего администратора
                    cursor_local.execute('UPDATE admins SET is_active = TRUE, added_by = %s, added_at = NOW() WHERE id = %s', (added_by, admin_id))
                    conn_local.commit()
                    return True, "Администратор активирован"
            else:
                # Создаем нового администратора
                cursor_local.execute('''
                    INSERT INTO admins (user_id, added_by, is_active)
                    VALUES (%s, %s, TRUE)
                ''', (user_id, added_by))
                conn_local.commit()
                return True, "Администратор добавлен"
    except Exception as e:
        logger.error(f"Ошибка при добавлении администратора: {e}", exc_info=True)
        return False, f"Ошибка при добавлении администратора: {e}"
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass


def remove_admin(user_id):
    """
    Удаляет администратора
    
    Args:
        user_id: ID администратора для удаления
    
    Returns:
        (success: bool, message: str)
    """
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        # Нельзя удалить владельца
        if is_owner(user_id):
            return False, "Нельзя удалить владельца бота"
        
        with db_lock:
            cursor_local.execute('UPDATE admins SET is_active = FALSE WHERE user_id = %s', (user_id,))
            if cursor_local.rowcount > 0:
                conn_local.commit()
                return True, "Администратор удален"
            else:
                return False, "Администратор не найден"
    except Exception as e:
        logger.error(f"Ошибка при удалении администратора: {e}", exc_info=True)
        return False, f"Ошибка при удалении администратора: {e}"
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass


def get_all_admins():
    """
    Получает список всех активных администраторов
    
    Returns:
        list: [{'user_id': int, 'added_by': int, 'added_at': datetime}]
    """
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        with db_lock:
            cursor_local.execute('''
                SELECT user_id, added_by, added_at
                FROM admins
                WHERE is_active = TRUE
                ORDER BY added_at DESC
            ''')
            rows = cursor_local.fetchall()
            
            result = []
            for row in rows:
                if isinstance(row, dict):
                    result.append({
                        'user_id': row['user_id'],
                        'added_by': row['added_by'],
                        'added_at': row['added_at']
                    })
                else:
                    result.append({
                        'user_id': row.get('user_id') if isinstance(row, dict) else row[0],
                        'added_by': row[1],
                        'added_at': row[2]
                    })
            return result
    except Exception as e:
        logger.error(f"Ошибка при получении списка администраторов: {e}", exc_info=True)
        return []
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass






