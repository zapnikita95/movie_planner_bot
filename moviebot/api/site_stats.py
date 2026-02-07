"""
Логика статистики для сайта: персональная и групповая.
"""
import logging
from datetime import datetime
from collections import defaultdict

import pytz
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock

logger = logging.getLogger(__name__)

MONTH_NAMES_RU = [
    'Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
    'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'
]

AVATAR_COLORS = ['#ff2d7b', '#9b4dff', '#00d4ff', '#34d399', '#fbbf24', '#fb923c', '#f472b6', '#a78bfa']

PLATFORM_MAP = {
    'kinopoisk.ru': 'Кинопоиск', 'www.kinopoisk.ru': 'Кинопоиск',
    'netflix.com': 'Netflix', 'www.netflix.com': 'Netflix',
    'okko.tv': 'Okko', 'www.okko.tv': 'Okko',
    'ivi.ru': 'Иви', 'www.ivi.ru': 'Иви',
    'more.tv': 'more.tv', 'www.more.tv': 'more.tv',
    'wink.ru': 'Wink', 'www.wink.ru': 'Wink',
    'start.video': 'Start', 'www.start.video': 'Start',
}


def _platform_from_link(link):
    if not link:
        return None
    link = str(link).lower()
    for domain, name in PLATFORM_MAP.items():
        if domain in link:
            return name
    if 'www.' in link:
        parts = link.split('www.')[-1].split('/')[0].split('.')
        if len(parts) >= 2:
            return parts[-2] + '.' + parts[-1]
    return None


def _month_range(month, year):
    """Возвращает (start_ts, end_ts) для месяца в UTC."""
    tz = pytz.UTC
    start = datetime(year, month, 1, 0, 0, 0, tzinfo=tz)
    if month == 12:
        end = datetime(year + 1, 1, 1, 0, 0, 0, tzinfo=tz)
    else:
        end = datetime(year, month + 1, 1, 0, 0, 0, tzinfo=tz)
    return start, end


def get_personal_stats(chat_id, month, year):
    """Персональная статистика за месяц. chat_id > 0."""
    conn = get_db_connection()
    cur = get_db_cursor()
    start_ts, end_ts = _month_range(month, year)
    user_id = chat_id  # для личного чата chat_id = user_id

    with db_lock:
        # Фильмы/сериалы с датой просмотра из watched_movies
        cur.execute("""
            SELECT wm.film_id, wm.watched_at, m.kp_id, m.title, m.year, m.genres, m.is_series, m.online_link
            FROM watched_movies wm
            JOIN movies m ON m.id = wm.film_id AND m.chat_id = wm.chat_id
            WHERE wm.chat_id = %s AND wm.user_id = %s
              AND wm.watched_at >= %s AND wm.watched_at < %s
        """, (chat_id, user_id, start_ts, end_ts))
        watched_rows = cur.fetchall()

        # Серии из series_tracking
        cur.execute("""
            SELECT st.film_id, st.watched_date, m.kp_id, m.title, m.is_series
            FROM series_tracking st
            JOIN movies m ON m.id = st.film_id AND m.chat_id = st.chat_id
            WHERE st.chat_id = %s AND st.user_id = %s AND st.watched = TRUE
              AND st.watched_date >= %s AND st.watched_date < %s
        """, (chat_id, user_id, start_ts, end_ts))
        series_rows = cur.fetchall()

        # Рейтинги
        cur.execute("""
            SELECT r.film_id, r.rating, r.rated_at, m.kp_id, m.title, m.year, m.genres, m.is_series
            FROM ratings r
            JOIN movies m ON m.id = r.film_id AND m.chat_id = r.chat_id
            WHERE r.chat_id = %s AND r.user_id = %s
        """, (chat_id, user_id))
        ratings_all = cur.fetchall()

    # Фильтруем рейтинги по месяцу если есть rated_at
    ratings_in_month = []
    for r in ratings_all:
        rt_at = r.get('rated_at') if isinstance(r, dict) else (r[2] if len(r) > 2 else None)
        if rt_at is not None:
            dt = _ensure_tz(rt_at)
            if dt and start_ts <= dt < end_ts:
                ratings_in_month.append(r)
        else:
            ratings_in_month.append(r)  # без даты — включаем (fallback)

    films_watched = set()
    series_watched = set()
    episodes_count = 0
    for r in series_rows:
        fid = r.get('film_id') if isinstance(r, dict) else r[0]
        is_series = r.get('is_series') if isinstance(r, dict) else r[4]
        if is_series:
            series_watched.add(fid)
            episodes_count += 1
        else:
            films_watched.add(fid)

    for r in watched_rows:
        fid = r.get('film_id') if isinstance(r, dict) else r[0]
        is_series = r.get('is_series') if isinstance(r, dict) else r[6]
        if is_series:
            series_watched.add(fid)
        else:
            films_watched.add(fid)

    # Cinema из plans
    with db_lock:
        cur.execute("""
            SELECT p.film_id, p.plan_datetime, m.kp_id, m.title, m.year
            FROM plans p
            JOIN movies m ON m.id = p.film_id AND m.chat_id = p.chat_id
            WHERE p.chat_id = %s AND p.plan_type = 'cinema'
              AND p.plan_datetime >= %s AND p.plan_datetime < %s
        """, (chat_id, start_ts, end_ts))
        cinema_rows = cur.fetchall()

    # Собираем watched для вывода (из watched_movies + series_tracking)
    watched_list = []
    seen = set()
    for r in watched_rows:
        fid = r.get('film_id') if isinstance(r, dict) else r[0]
        kp_id = r.get('kp_id') if isinstance(r, dict) else r[2]
        title = r.get('title') if isinstance(r, dict) else r[3]
        year = r.get('year') if isinstance(r, dict) else r[4]
        is_series = r.get('is_series') if isinstance(r, dict) else r[6]
        watched_at = r.get('watched_at') if isinstance(r, dict) else r[1]
        online_link = r.get('online_link') if isinstance(r, dict) else (r[7] if len(r) > 7 else None)
        key = (fid, 'wm')
        if key not in seen:
            seen.add(key)
            rating = next((x.get('rating') for x in ratings_in_month if (x.get('film_id') == fid)), None)
            watched_list.append({
                'film_id': fid, 'kp_id': kp_id, 'title': title, 'year': year,
                'type': 'series' if is_series else 'film',
                'date': watched_at.strftime('%Y-%m-%d') if hasattr(watched_at, 'strftime') else str(watched_at)[:10],
                'rating': rating, 'is_cinema': False,
                'online_link': online_link
            })

    # Платформы из online_link
    platform_counts = defaultdict(int)
    for w in watched_list:
        if w.get('is_cinema'):
            platform_counts['Кинотеатр'] += 1
        else:
            plat = _platform_from_link(w.get('online_link'))
            if plat:
                platform_counts[plat] += 1
    platforms = [{'platform': k, 'count': v} for k, v in sorted(platform_counts.items(), key=lambda x: -x[1])]

    # rating_breakdown
    rating_breakdown = {str(i): 0 for i in range(1, 11)}
    for r in ratings_in_month:
        rt = r.get('rating') if isinstance(r, dict) else r[1]
        if rt and 1 <= rt <= 10:
            rating_breakdown[str(int(rt))] = rating_breakdown.get(str(int(rt)), 0) + 1

    # top_films по рейтингу
    top_films = []
    for r in ratings_in_month:
        rt = r.get('rating') if isinstance(r, dict) else r[1]
        kp_id = r.get('kp_id') if isinstance(r, dict) else r[4]
        title = r.get('title') if isinstance(r, dict) else r[5]
        year = r.get('year') if isinstance(r, dict) else r[6]
        genres = r.get('genres') if isinstance(r, dict) else r[7]
        fid = r.get('film_id') if isinstance(r, dict) else r[0]
        top_films.append({'film_id': fid, 'kp_id': kp_id, 'title': title, 'year': year, 'rating': rt, 'genre': genres or ''})
    top_films.sort(key=lambda x: (-(x.get('rating') or 0), x.get('title') or ''))
    top_films = top_films[:10]

    # cinema list
    cinema_list = []
    for r in cinema_rows:
        fid = r.get('film_id') if isinstance(r, dict) else r[0]
        dt = r.get('plan_datetime') if isinstance(r, dict) else r[1]
        kp_id = r.get('kp_id') if isinstance(r, dict) else r[2]
        title = r.get('title') if isinstance(r, dict) else r[3]
        year = r.get('year') if isinstance(r, dict) else r[4]
        date_str = dt.strftime('%Y-%m-%d') if hasattr(dt, 'strftime') else str(dt)[:10]
        rating = next((x.get('rating') for x in ratings_in_month if x.get('film_id') == fid), None)
        cinema_list.append({'film_id': fid, 'kp_id': kp_id, 'title': title, 'year': year, 'date': date_str, 'place': None, 'rating': rating})

    # avg_rating
    total_rating = sum(r.get('rating') or 0 for r in ratings_in_month)
    count_rating = len(ratings_in_month)
    avg_rating = round(total_rating / count_rating, 1) if count_rating else None

    return {
        'period': {'month': month, 'year': year, 'label': f'{MONTH_NAMES_RU[month - 1]} {year}'},
        'summary': {
            'films_watched': len(films_watched),
            'series_watched': len(series_watched),
            'episodes_watched': episodes_count,
            'cinema_visits': len(cinema_rows),
            'total_watched': len(films_watched) + len(series_watched),
            'avg_rating': avg_rating
        },
        'top_films': top_films,
        'cinema': cinema_list,
        'platforms': platforms,
        'watched': watched_list[:50],
        'rating_breakdown': rating_breakdown
    }


def _ensure_tz(dt):
    if dt is None:
        return None
    if hasattr(dt, 'tzinfo') and dt.tzinfo:
        return dt
    return pytz.UTC.localize(dt) if hasattr(dt, 'replace') else dt


def get_group_stats(chat_id, month, year):
    """Групповая статистика. chat_id < 0."""
    conn = get_db_connection()
    cur = get_db_cursor()
    start_ts, end_ts = _month_range(month, year)

    with db_lock:
        cur.execute("SELECT name FROM site_sessions WHERE chat_id = %s ORDER BY created_at DESC LIMIT 1", (chat_id,))
        row = cur.fetchone()
    group_title = (row.get('name') if isinstance(row, dict) else (row[0] if row else None)) if row else 'Группа'

    # Участники из ratings и watched_movies
    with db_lock:
        cur.execute("SELECT DISTINCT user_id FROM ratings WHERE chat_id = %s", (chat_id,))
        user_ids = {r.get('user_id') if isinstance(r, dict) else r[0] for r in cur.fetchall()}
        cur.execute("SELECT DISTINCT user_id FROM watched_movies WHERE chat_id = %s", (chat_id,))
        for r in cur.fetchall():
            user_ids.add(r.get('user_id') if isinstance(r, dict) else r[0])
        cur.execute("SELECT DISTINCT user_id FROM series_tracking WHERE chat_id = %s AND watched = TRUE", (chat_id,))
        for r in cur.fetchall():
            user_ids.add(r.get('user_id') if isinstance(r, dict) else r[0])

    user_ids = [u for u in user_ids if u]
    if not user_ids:
        return _empty_group_response(chat_id, group_title, month, year)

    # Цвета аватаров
    avatar_colors = {}
    with db_lock:
        cur.execute("SELECT user_id, color FROM member_avatar_colors WHERE chat_id = %s", (chat_id,))
        for r in cur.fetchall():
            if isinstance(r, dict):
                avatar_colors[r.get('user_id')] = r.get('color')
            else:
                avatar_colors[r[0]] = r[1]

    members = []
    for i, uid in enumerate(user_ids):
        color = avatar_colors.get(uid) or AVATAR_COLORS[i % len(AVATAR_COLORS)]
        members.append({'user_id': uid, 'username': None, 'first_name': f'Участник {uid}', 'avatar_color': color})

    # Упрощённая агрегация
    with db_lock:
        cur.execute("SELECT COUNT(*) FROM movies WHERE chat_id = %s", (chat_id,))
        cnt_row = cur.fetchone()
        total_films = (cnt_row.get('count') if isinstance(cnt_row, dict) else cnt_row[0]) or 0

    summary = {
        'group_films': 0, 'group_ratings': 0, 'group_cinema': 0,
        'group_series': 0, 'group_episodes': 0, 'active_members': len(user_ids)
    }
    rating_breakdown = {str(i): 0 for i in range(1, 11)}
    top_films = []
    leaderboard = {'watched': [], 'ratings': [], 'avg_rating': [], 'cinema': []}
    controversial = []
    compatibility = []
    genres = []
    achievements = []
    heatmap = {}

    with db_lock:
        cur.execute("""
            SELECT r.rating, r.user_id FROM ratings r
            WHERE r.chat_id = %s
        """, (chat_id,))
        ratings_all = cur.fetchall()

    for r in ratings_all:
        rt = r.get('rating') if isinstance(r, dict) else r[0]
        if rt and 1 <= rt <= 10:
            rating_breakdown[str(int(rt))] = rating_breakdown.get(str(int(rt)), 0) + 1
    summary['group_ratings'] = sum(rating_breakdown.values())

    # MVP и achievements
    uid_counts = defaultdict(int)
    for r in ratings_all:
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        uid_counts[uid] += 1
    mvp_uid = None
    if uid_counts:
        mvp_uid = max(uid_counts, key=uid_counts.get)
        rater_count = uid_counts.get(mvp_uid, 0)
        achievements = [
            {'id': 'cinephile', 'icon': '🎬', 'name': 'Киноман', 'description': 'Посмотрел 10+ фильмов за месяц', 'holder_user_id': mvp_uid, 'earned': rater_count >= 10},
            {'id': 'rater', 'icon': '📊', 'name': 'Оценщик', 'description': '15+ оценок за месяц', 'holder_user_id': mvp_uid if rater_count >= 15 else None, 'earned': rater_count >= 15},
        ]
    else:
        achievements = [
            {'id': 'cinephile', 'icon': '🎬', 'name': 'Киноман', 'description': 'Посмотрел 10+ фильмов за месяц', 'holder_user_id': None, 'earned': False},
            {'id': 'rater', 'icon': '📊', 'name': 'Оценщик', 'description': '15+ оценок за месяц', 'holder_user_id': None, 'earned': False},
        ]

    return {
        'success': True,
        'group': {
            'chat_id': chat_id,
            'title': group_title,
            'members_active': len(members),
            'total_films_alltime': total_films,
            'public_slug': None
        },
        'period': {'month': month, 'year': year, 'label': f'{MONTH_NAMES_RU[month - 1]} {year}'},
        'members': members,
        'summary': summary,
        'mvp': {'user_id': mvp_uid, 'films': uid_counts.get(mvp_uid, 0), 'ratings': uid_counts.get(mvp_uid, 0), 'avg_rating': 7.0, 'reason': 'most_active'} if uid_counts else None,
        'top_films': top_films,
        'rating_breakdown': rating_breakdown,
        'leaderboard': leaderboard,
        'controversial': controversial,
        'compatibility': compatibility,
        'genres': genres,
        'achievements': achievements,
        'activity_heatmap': heatmap
    }


def _empty_group_response(chat_id, title, month, year):
    return {
        'success': True,
        'group': {'chat_id': chat_id, 'title': title, 'members_active': 0, 'total_films_alltime': 0, 'public_slug': None},
        'period': {'month': month, 'year': year, 'label': f'{MONTH_NAMES_RU[month - 1]} {year}'},
        'members': [],
        'summary': {'group_films': 0, 'group_ratings': 0, 'group_cinema': 0, 'group_series': 0, 'group_episodes': 0, 'active_members': 0},
        'mvp': None,
        'top_films': [],
        'rating_breakdown': {str(i): 0 for i in range(1, 11)},
        'leaderboard': {'watched': [], 'ratings': [], 'avg_rating': [], 'cinema': []},
        'controversial': [],
        'compatibility': [],
        'genres': [],
        'achievements': [],
        'activity_heatmap': {}
    }


def get_public_group_stats(slug, month, year):
    """Публичная групповая статистика по slug."""
    conn = get_db_connection()
    cur = get_db_cursor()
    with db_lock:
        cur.execute("SELECT chat_id, public_enabled, visible_blocks FROM group_stats_settings WHERE public_slug = %s", (slug,))
        row = cur.fetchone()
    if not row:
        return None, 'Group not found'
    chat_id = row.get('chat_id') if isinstance(row, dict) else row[0]
    public_enabled = row.get('public_enabled') if isinstance(row, dict) else row[1]
    if not public_enabled:
        return None, 'Stats are private'
    data = get_group_stats(chat_id, month, year)
    data['group']['public_slug'] = slug
    return data, None
