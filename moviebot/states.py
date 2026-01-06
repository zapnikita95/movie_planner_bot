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
clean_unwatched_votes = {}  # message_id: {'chat_id': int, 'members_count': int, 'voted': set}

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
user_view_film_state = {}  # user_id: {'chat_id': int} - состояние ожидания ответного сообщения для просмотра страницы фильма

# Сообщения "Добавлено в базу" для обработки реплаев с оценками
added_movie_messages = {}  # message_id: {'chat_id': int, 'film_id': int, 'kp_id': str, 'link': str, 'title': str}

# Состояния оплаты
user_payment_state = {}  # user_id: {'step': str, 'subscription_type': str, 'plan_type': str, 'period_type': str, 'chat_id': int, 'group_username': str, 'telegram_username': str}
user_cancel_subscription_state = {}  # user_id: {'subscription_id': int, 'subscription_type': str, 'chat_id': int}

# Состояния работы с сериалами
user_episodes_state = {}  # user_id: {'kp_id': str, 'season_num': int, 'episodes': list, ...}

# Сообщения для обработки оценок
rating_messages = {}  # message_id: film_id (для обработки реплаев с оценками)

# Состояния игры с кубиком
dice_game_state = {}  # chat_id: {'participants': {user_id: dice_value}, 'message_id': int, 'start_time': datetime}

# Состояния возврата звезд
user_refund_state = {}  # user_id: {'chat_id': int} - состояние ожидания ввода charge_id для возврата

# Состояния промокодов
user_promo_state = {}  # user_id: {'chat_id': int, 'message_id': int, 'sub_type': str, 'plan_type': str, 'period_type': str, 'group_size': int or None, 'payment_id': str, 'original_price': float} - состояние ожидания ввода промокода
user_promo_admin_state = {}  # user_id: {} - состояние ожидания ввода промокода для /promo

# Состояния админских команд
user_unsubscribe_state = {}  # user_id: {'message_id': int} - состояние ожидания ввода ID для /unsubscribe
user_add_admin_state = {}  # user_id: {'message_id': int} - состояние ожидания ввода ID для /add_admin

# Состояния для личных сообщений (чтобы следующее сообщение без реплая обрабатывалось как ответ на prompt)
private_chat_prompts = {}  # user_id: {'prompt_message_id': int, 'handler_type': str} - для обработки следующего сообщения в личке как реплая

