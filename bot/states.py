"""
Модуль для глобальных состояний бота
"""

# Состояния планирования
user_plan_state = {}  # user_id: {'step': int, 'link': str, 'type': str, 'day_or_date': str}
bot_messages = {}  # message_id: link (храним карточки бота)
plan_notification_messages = {}  # message_id: {'link': str} (храним сообщения о планах для обработки реакций)
list_messages = {}  # message_id: chat_id (храним сообщения /list для обработки ответов)
plan_error_messages = {}  # message_id: {'user_id': int, 'chat_id': int, 'link': str, 'plan_type': str or None, 'day_or_date': str or None, 'missing': str}

# Состояния настроек
user_settings_state = {}  # user_id: {'waiting_emoji': bool}
settings_messages = {}  # message_id: {'user_id': int, 'action': str, 'chat_id': int} - для отслеживания сообщений settings
user_import_state = {}  # user_id: {'step': str, 'kp_user_id': str, 'count': int} - для импорта базы из Кинопоиска

# Состояния очистки
user_clean_state = {}  # user_id: {'action': str, 'target': str}
clean_votes = {}  # message_id: {'chat_id': int, 'members_count': int, 'voted': set}

# Состояния редактирования
user_edit_state = {}  # user_id: {'action': str, 'plan_id': int, 'step': str, ...}

# Состояния работы с билетами
user_ticket_state = {}  # user_id: {'step': str, 'plan_id': int, 'file_id': str, ...}

# Состояния поиска
user_search_state = {}  # user_id: {'chat_id': int, 'message_id': int}

# Состояния рандома
user_random_state = {}  # user_id: {'step': str, 'mode': str, ...}

# Состояния списка
user_list_state = {}  # user_id: {'chat_id': int, 'page': int}

