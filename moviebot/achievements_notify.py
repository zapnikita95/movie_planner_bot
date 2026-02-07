"""
Мгновенные уведомления о получении ачивок.
Вызывается сразу после действий пользователя (оценка, просмотр, кино, серия).
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# reason_text по ачивкам. {ach_id: template}. {film_title} подставляется из context.
REASON_TEXTS = {
    'films_1': 'Ты посмотрел свой первый фильм в Movie Planner — «{film_title}»!',
    'films_5': 'Ты посмотрел уже 5 фильмов! Так держать!',
    'films_10': '10 фильмов в твоей базе — отличное начало!',
    'films_50': 'Ты посмотрел уже 50 фильмов! Так держать!',
    'films_100': '100 фильмов в Movie Planner — серьёзная цифра!',
    'films_250': '250 фильмов! Четверть тысячи — это впечатляет.',
    'films_500': '500 фильмов. Ты — настоящая легенда кино.',
    'ratings_1': 'Ты поставил первую оценку — «{film_title}»!',
    'ratings_10': 'Уже 10 оценок — твоё мнение формируется!',
    'ratings_50': '50 оценок — ты активный критик!',
    'ratings_100': 'Ты поставил 100 оценок! Твоё мнение ценно.',
    'ratings_500': '500 оценок — ты настоящий эксперт!',
    'cinema_1': 'Первый поход в кино через Movie Planner — «{film_title}»!',
    'cinema_10': '10 походов в кино! Ты настоящий кинозритель.',
    'cinema_25': '25 раз в кино — ты завсегдатай!',
    'cinema_50': '50 походов в кино — синефил со стажем!',
    'cinema_100': '100 раз в кино! Ты — кинофанат в чистом виде.',
    'series_completed_1': 'Первый сериал досмотрен до конца!',
    'series_completed_3': '3 сериала завершены — ты в теме!',
    'series_completed_5': '5 сериалов досмотрено до конца!',
    'series_completed_10': '10 сериалов! Настоящий сериальный марафонец.',
    'series_ep_50': '50 серий отмечено — трекинг идёт!',
    'series_ep_100': '100 серий! Ты следишь за прогрессом.',
    'series_ep_250': '250 серий — серьёзный прогресс!',
    'series_ep_500': '500 серий отмечено — сериальный марафонец!',
    'series_ep_1000': '1000 серий! Легенда трекинга.',
    'genres_3': 'Ты смотришь кино из 3+ жанров — разнообразие!',
    'genres_5': '5 жанров в твоих оценках — широкий вкус!',
    'genres_10': 'Ты смотришь кино из 10+ жанров — всеядный зритель!',
    'genres_15': '15 жанров! Ты настоящий универсал.',
    'plans_1': 'Первый план создан — «{film_title}»!',
    'plans_5': '5 планов — ты планируешь просмотры!',
    'plans_10': '10 планов — настоящий организатор!',
    'plans_25': '25 планов — мастер планирования!',
    'year_streak': '12 месяцев подряд ты смотришь кино. Годовой стрик!',
    'oldtimer': 'Ты с нами уже больше года. Спасибо, что остаёшься!',
    'mvp_legend': 'Ты стал «Киноманом месяца» уже 6 раз! Легенда.',
    'collector': 'У тебя уже 10 ачивок — коллекция растёт!',
}

RARITY_PREFIX = {
    'common': '🏆 Новая ачивка!',
    'rare': '💎 Редкая ачивка!',
    'epic': '🔥 Эпическая ачивка!',
    'legendary': '👑✨ ЛЕГЕНДАРНАЯ АЧИВКА!',
}


def _get_reason_text(ach_id: str, context: Optional[dict]) -> str:
    template = REASON_TEXTS.get(ach_id, '')
    if not template:
        return ''
    film_title = (context or {}).get('film_title') or 'фильм'
    return template.format(film_title=film_title)


def notify_new_achievements(user_id: int, context: Optional[dict] = None):
    """
    Проверяет ачивки пользователя и отправляет уведомления о новых.
    context: {film_title, is_cinema} — для кастомизации reason_text.
    """
    try:
        from moviebot.api.site_stats import _get_user_profile_and_achievements
        from moviebot.bot.bot_init import bot
        from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
        from moviebot.scheduler import _onboarding_was_sent, _onboarding_set_sent, _user_has_blocked_bot
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
    except ImportError as e:
        logger.warning(f"[ACHIEVEMENT NOTIFY] Import error: {e}")
        return

    if not bot:
        return

    context = context or {}
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        if _user_has_blocked_bot(user_id, cursor):
            return

        _, achievements = _get_user_profile_and_achievements(user_id)
        newly_earned = []
        for ach in achievements:
            if not ach.get('earned'):
                continue
            ach_id = ach.get('id')
            key = f"achievement_notified_{ach_id}"
            if _onboarding_was_sent(user_id, key, cursor):
                continue
            newly_earned.append(ach)

        if not newly_earned:
            return

        # Rarity prefix — от самой редкой среди новых
        rarity_order = {'common': 0, 'rare': 1, 'epic': 2, 'legendary': 3}
        max_rarity = max((ach.get('rarity', 'common') for ach in newly_earned), key=lambda r: rarity_order.get(r, 0))
        prefix = RARITY_PREFIX.get(max_rarity, RARITY_PREFIX['common'])

        if len(newly_earned) == 1:
            a = newly_earned[0]
            body = f"{a.get('icon', '🏆')} {a.get('name', '')}\n{a.get('description', '')}\n\n"
            reason = _get_reason_text(a.get('id', ''), context)
            if reason:
                body += reason
            else:
                body += f"Ты выполнил условие ачивки «{a.get('name', '')}»!"
        else:
            body = "Сразу {} новых ачивок!\n\n".format(len(newly_earned))
            for a in newly_earned:
                body += f"{a.get('icon', '🏆')} {a.get('name', '')}\n{a.get('description', '')}\n\n"
            body += "Продолжай в том же духе!"

        text = f"{prefix}\n\n{body}"

        markup = InlineKeyboardMarkup(row_width=1)
        try:
            bot_username = bot.get_me().username
        except Exception:
            bot_username = None
        if bot_username:
            markup.add(InlineKeyboardButton(
                "📊 Перейти в личный кабинет",
                callback_data="send_login_code"
            ))

        try:
            bot.send_message(user_id, text, reply_markup=markup, parse_mode='HTML')
            for a in newly_earned:
                _onboarding_set_sent(user_id, f"achievement_notified_{a.get('id', '')}")
            logger.info(f"[ACHIEVEMENT NOTIFY] Отправлено user_id={user_id}, ачивки: {[a.get('id') for a in newly_earned]}")
        except Exception as e:
            logger.warning(f"[ACHIEVEMENT NOTIFY] Не удалось отправить user_id={user_id}: {e}")
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
