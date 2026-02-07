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

    # Участники из ratings, watched_movies, series_tracking, plans
    user_ids = set()
    with db_lock:
        for q in [
            "SELECT DISTINCT user_id FROM ratings WHERE chat_id = %s",
            "SELECT DISTINCT user_id FROM watched_movies WHERE chat_id = %s",
            "SELECT DISTINCT user_id FROM series_tracking WHERE chat_id = %s AND watched = TRUE",
            "SELECT DISTINCT user_id FROM plans WHERE chat_id = %s AND user_id IS NOT NULL",
        ]:
            cur.execute(q, (chat_id,))
            for r in cur.fetchall():
                uid = r.get('user_id') if isinstance(r, dict) else r[0]
                if uid:
                    user_ids.add(uid)
    user_ids = list(user_ids)
    if not user_ids:
        return _empty_group_response(chat_id, group_title, month, year)

    # Usernames из stats (последний username по user_id)
    usernames = {}
    with db_lock:
        cur.execute("""
            SELECT DISTINCT ON (user_id) user_id, username
            FROM stats
            WHERE chat_id = %s AND user_id = ANY(%s) AND username IS NOT NULL AND username != ''
            ORDER BY user_id, id DESC
        """, (chat_id, user_ids))
        for r in cur.fetchall():
            uid = r.get('user_id') if isinstance(r, dict) else r[0]
            un = r.get('username') if isinstance(r, dict) else r[1]
            if un and not un.startswith('@'):
                un = '@' + un
            usernames[uid] = un or None

    # Цвета аватаров
    avatar_colors = {}
    with db_lock:
        cur.execute("SELECT user_id, color FROM member_avatar_colors WHERE chat_id = %s", (chat_id,))
        for r in cur.fetchall():
            uid = r.get('user_id') if isinstance(r, dict) else r[0]
            avatar_colors[uid] = r.get('color') if isinstance(r, dict) else r[1]

    members = []
    for i, uid in enumerate(user_ids):
        color = avatar_colors.get(uid) or AVATAR_COLORS[i % len(AVATAR_COLORS)]
        display_name = usernames.get(uid) or f'Участник {uid}'
        members.append({
            'user_id': uid, 'username': usernames.get(uid),
            'first_name': display_name,
            'avatar_color': color
        })

    # Уникальные фильмы в базе группы
    with db_lock:
        cur.execute("SELECT COUNT(*) FROM movies WHERE chat_id = %s", (chat_id,))
        cnt_row = cur.fetchone()
        total_films = (cnt_row.get('count') if isinstance(cnt_row, dict) else cnt_row[0]) or 0

    # watched_movies за месяц (фильмы + сериалы)
    with db_lock:
        cur.execute("""
            SELECT wm.film_id, wm.user_id, m.kp_id, m.title, m.year, m.is_series, wm.watched_at
            FROM watched_movies wm
            JOIN movies m ON m.id = wm.film_id AND m.chat_id = wm.chat_id
            WHERE wm.chat_id = %s AND wm.watched_at >= %s AND wm.watched_at < %s
        """, (chat_id, start_ts, end_ts))
        wm_rows = cur.fetchall()

    # series_tracking за месяц
    with db_lock:
        cur.execute("""
            SELECT st.film_id, st.user_id, m.kp_id, m.title, m.year, m.is_series, st.watched_date
            FROM series_tracking st
            JOIN movies m ON m.id = st.film_id AND m.chat_id = st.chat_id
            WHERE st.chat_id = %s AND st.watched = TRUE
              AND st.watched_date >= %s AND st.watched_date < %s
        """, (chat_id, start_ts, end_ts))
        st_rows = cur.fetchall()

    # plans cinema за месяц
    with db_lock:
        cur.execute("""
            SELECT p.film_id, p.user_id, p.plan_datetime, m.kp_id, m.title, m.year
            FROM plans p
            JOIN movies m ON m.id = p.film_id AND m.chat_id = p.chat_id
            WHERE p.chat_id = %s AND p.plan_type = 'cinema'
              AND p.plan_datetime >= %s AND p.plan_datetime < %s
        """, (chat_id, start_ts, end_ts))
        cinema_rows = cur.fetchall()

    # ratings за месяц (rated_at если есть)
    with db_lock:
        cur.execute("""
            SELECT r.rating, r.user_id, r.film_id, r.rated_at, m.kp_id, m.title, m.year, m.genres
            FROM ratings r
            JOIN movies m ON m.id = r.film_id AND m.chat_id = r.chat_id
            WHERE r.chat_id = %s
        """, (chat_id,))
        ratings_all = cur.fetchall()

    # Фильтруем ratings по месяцу
    ratings_in_month = []
    for r in ratings_all:
        rt_at = r.get('rated_at') if isinstance(r, dict) else (r[3] if len(r) > 3 else None)
        if rt_at is not None:
            dt = _ensure_tz(rt_at)
            if dt and start_ts <= dt < end_ts:
                ratings_in_month.append(r)
        else:
            ratings_in_month.append(r)

    # Агрегация: group_films (уникальные фильмы), group_series, group_episodes
    films_watched = set()  # film_id (не сериалы)
    series_watched = set()
    episodes_count = 0
    uid_watched = defaultdict(int)
    for r in wm_rows:
        fid = r.get('film_id') if isinstance(r, dict) else r[0]
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        is_series = r.get('is_series') if isinstance(r, dict) else r[5]
        if is_series:
            series_watched.add(fid)
            episodes_count += 1
        else:
            films_watched.add(fid)
        uid_watched[uid] += 1
    for r in st_rows:
        fid = r.get('film_id') if isinstance(r, dict) else r[0]
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        is_series = r.get('is_series') if isinstance(r, dict) else r[4]
        if is_series:
            series_watched.add(fid)
            episodes_count += 1
        else:
            films_watched.add(fid)
        uid_watched[uid] += 1

    group_films_count = len(films_watched) + len(series_watched)

    # Cinema count
    uid_cinema = defaultdict(int)
    for r in cinema_rows:
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        if uid:
            uid_cinema[uid] += 1
    group_cinema_count = len(cinema_rows)

    # Ratings per user
    uid_ratings = defaultdict(list)
    for r in ratings_in_month:
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        rt = r.get('rating') if isinstance(r, dict) else r[0]
        if uid and rt and 1 <= rt <= 10:
            uid_ratings[uid].append(rt)

    rating_breakdown = {str(i): 0 for i in range(1, 11)}
    for r in ratings_in_month:
        rt = r.get('rating') if isinstance(r, dict) else r[0]
        if rt and 1 <= rt <= 10:
            rating_breakdown[str(int(rt))] = rating_breakdown.get(str(int(rt)), 0) + 1

    summary = {
        'group_films': group_films_count,
        'group_ratings': len(ratings_in_month),
        'group_cinema': group_cinema_count,
        'group_series': len(series_watched),
        'group_episodes': episodes_count,
        'active_members': len(user_ids)
    }

    # Leaderboard
    lb_watched = [{'user_id': uid, 'count': c} for uid, c in sorted(uid_watched.items(), key=lambda x: -x[1])]
    lb_ratings = [{'user_id': uid, 'count': len(rats)} for uid, rats in sorted(uid_ratings.items(), key=lambda x: -len(x[1]))]
    lb_avg = []
    for uid, rats in uid_ratings.items():
        avg = round(sum(rats) / len(rats), 1) if rats else 0
        lb_avg.append({'user_id': uid, 'value': avg})
    lb_avg.sort(key=lambda x: -x['value'])
    lb_cinema = [{'user_id': uid, 'count': c} for uid, c in sorted(uid_cinema.items(), key=lambda x: -x[1])]
    leaderboard = {'watched': lb_watched, 'ratings': lb_ratings, 'avg_rating': lb_avg, 'cinema': lb_cinema}

    # MVP: кто больше всего смотрел + оценивал
    uid_activity = defaultdict(lambda: {'watched': 0, 'ratings': 0, 'rating_sum': 0})
    for uid, c in uid_watched.items():
        uid_activity[uid]['watched'] = c
    for uid, rats in uid_ratings.items():
        uid_activity[uid]['ratings'] = len(rats)
        uid_activity[uid]['rating_sum'] = sum(rats)
    mvp_uid = None
    mvp_films = 0
    mvp_ratings = 0
    mvp_avg = 7.0
    if uid_activity:
        mvp_uid = max(uid_activity, key=lambda u: (uid_activity[u]['watched'] + uid_activity[u]['ratings']))
        mvp_films = uid_activity[mvp_uid]['watched']
        mvp_ratings = uid_activity[mvp_uid]['ratings']
        if uid_activity[mvp_uid]['ratings']:
            mvp_avg = round(uid_activity[mvp_uid]['rating_sum'] / uid_activity[mvp_uid]['ratings'], 1)

    achievements = [
        {'id': 'cinephile', 'icon': '🎬', 'name': 'Киноман', 'description': 'Посмотрел 10+ фильмов за месяц',
         'holder_user_id': mvp_uid if mvp_films >= 10 else None, 'earned': mvp_films >= 10},
        {'id': 'rater', 'icon': '📊', 'name': 'Оценщик', 'description': '15+ оценок за месяц',
         'holder_user_id': mvp_uid if mvp_ratings >= 15 else None, 'earned': mvp_ratings >= 15},
    ]

    # top_films по средней оценке (фильмы с несколькими оценками)
    film_ratings = defaultdict(list)
    for r in ratings_in_month:
        fid = r.get('film_id') if isinstance(r, dict) else r[2]
        rt = r.get('rating') if isinstance(r, dict) else r[0]
        kp_id = r.get('kp_id') if isinstance(r, dict) else r[4]
        title = r.get('title') if isinstance(r, dict) else r[5]
        year = r.get('year') if isinstance(r, dict) else r[6]
        genres = r.get('genres') if isinstance(r, dict) else r[7]
        if fid and rt:
            film_ratings[fid].append({'rating': rt, 'kp_id': kp_id, 'title': title, 'year': year, 'genre': genres or ''})
    top_films = []
    for fid, rats in film_ratings.items():
        avg = round(sum(r['rating'] for r in rats) / len(rats), 1)
        r0 = rats[0]
        top_films.append({
            'film_id': fid, 'kp_id': r0.get('kp_id'), 'title': r0.get('title'), 'year': r0.get('year'),
            'genre': r0.get('genre', ''), 'avg_rating': avg,
            'rated_by': [{'user_id': None, 'rating': r['rating']} for r in rats]
        })
    top_films.sort(key=lambda x: (-(x.get('avg_rating') or 0), x.get('title') or ''))
    top_films = top_films[:10]

    # Watched list для группы (всё просмотренное за месяц)
    watched_list = []
    seen = set()
    def add_watched(fid, kp_id, title, year_val, is_series, date_val, uid):
        key = (fid, uid, date_val)
        if key in seen:
            return
        seen.add(key)
        rating = next((r.get('rating') for r in ratings_in_month if r.get('film_id') == fid and r.get('user_id') == uid), None)
        watched_list.append({
            'film_id': fid, 'kp_id': kp_id, 'title': title, 'year': year_val,
            'type': 'series' if is_series else 'film',
            'date': date_val.strftime('%Y-%m-%d') if hasattr(date_val, 'strftime') else str(date_val)[:10],
            'rating': rating, 'user_id': uid
        })
    for r in wm_rows:
        fid = r.get('film_id') if isinstance(r, dict) else r[0]
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        kp_id = r.get('kp_id') if isinstance(r, dict) else r[2]
        title = r.get('title') if isinstance(r, dict) else r[3]
        year = r.get('year') if isinstance(r, dict) else r[4]
        is_series = r.get('is_series') if isinstance(r, dict) else r[5]
        dt = r.get('watched_at') if isinstance(r, dict) else r[6]
        add_watched(fid, kp_id, title, year, is_series, dt, uid)
    for r in st_rows:
        fid = r.get('film_id') if isinstance(r, dict) else r[0]
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        kp_id = r.get('kp_id') if isinstance(r, dict) else r[2]
        title = r.get('title') if isinstance(r, dict) else r[3]
        year = r.get('year') if isinstance(r, dict) else (r[4] if len(r) > 4 else None)
        is_series = r.get('is_series') if isinstance(r, dict) else r[5]
        dt = r.get('watched_date') if isinstance(r, dict) else r[6]
        add_watched(fid, kp_id, title, year_val=year, is_series=is_series, date_val=dt, uid=uid)
    watched_list.sort(key=lambda x: (x.get('date') or '', x.get('title') or ''))
    watched_list = watched_list[:50]

    # Cinema list
    cinema_list = []
    for r in cinema_rows:
        fid = r.get('film_id') if isinstance(r, dict) else r[0]
        dt = r.get('plan_datetime') if isinstance(r, dict) else r[2]
        kp_id = r.get('kp_id') if isinstance(r, dict) else r[3]
        title = r.get('title') if isinstance(r, dict) else r[4]
        year = r.get('year') if isinstance(r, dict) else r[5]
        date_str = dt.strftime('%Y-%m-%d') if hasattr(dt, 'strftime') else str(dt)[:10]
        rating = next((x.get('rating') for x in ratings_in_month if x.get('film_id') == fid), None)
        cinema_list.append({'film_id': fid, 'kp_id': kp_id, 'title': title, 'year': year, 'date': date_str, 'rating': rating})

    controversial = []
    compatibility = []
    genres = []
    heatmap = {}

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
        'mvp': {'user_id': mvp_uid, 'films': mvp_films, 'ratings': mvp_ratings, 'avg_rating': mvp_avg, 'reason': 'most_active'} if mvp_uid is not None else None,
        'top_films': top_films,
        'rating_breakdown': rating_breakdown,
        'leaderboard': leaderboard,
        'controversial': controversial,
        'compatibility': compatibility,
        'genres': genres,
        'achievements': achievements,
        'activity_heatmap': heatmap,
        'watched': watched_list,
        'cinema': cinema_list
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


def get_user_stats_settings(user_id):
    """Настройки публичности личной статистики."""
    cur = get_db_cursor()
    with db_lock:
        cur.execute(
            "SELECT public_enabled, public_slug, visible_blocks FROM user_stats_settings WHERE user_id = %s",
            (user_id,)
        )
        row = cur.fetchone()
    if not row:
        return {
            'public_enabled': False,
            'public_slug': None,
            'visible_blocks': {
                'summary': True, 'top_films': True, 'rating_breakdown': True,
                'cinema': True, 'platforms': True, 'watched_list': True
            }
        }
    vb = row.get('visible_blocks') if isinstance(row, dict) else row[2]
    if isinstance(vb, str):
        import json
        vb = json.loads(vb) if vb else {}
    return {
        'public_enabled': bool(row.get('public_enabled') if isinstance(row, dict) else row[0]),
        'public_slug': row.get('public_slug') if isinstance(row, dict) else row[1],
        'visible_blocks': vb or {
            'summary': True, 'top_films': True, 'rating_breakdown': True,
            'cinema': True, 'platforms': True, 'watched_list': True
        }
    }


def set_user_stats_settings(user_id, public_enabled=None, visible_blocks=None):
    """Обновить настройки публичности. Создаёт slug при первом включении."""
    cur = get_db_cursor()
    import json
    with db_lock:
        cur.execute("SELECT public_enabled, public_slug, visible_blocks FROM user_stats_settings WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        if not row:
            slug = None
            if public_enabled:
                # Получить username из stats
                cur.execute(
                    "SELECT username FROM stats WHERE chat_id = %s AND user_id = %s AND username IS NOT NULL AND username != '' ORDER BY id DESC LIMIT 1",
                    (user_id, user_id)
                )
                un_row = cur.fetchone()
                un = (un_row.get('username') if isinstance(un_row, dict) else un_row[0]) if un_row else None
                slug = (un or '').lstrip('@').lower().replace(' ', '_')[:32] if un else f'user_{user_id}'
                # Проверить уникальность
                cur.execute("SELECT 1 FROM user_stats_settings WHERE public_slug = %s", (slug,))
                if cur.fetchone():
                    slug = f'user_{user_id}'
            vb = visible_blocks if visible_blocks is not None else {
                'summary': True, 'top_films': True, 'rating_breakdown': True,
                'cinema': True, 'platforms': True, 'watched_list': True
            }
            cur.execute(
                """INSERT INTO user_stats_settings (user_id, public_enabled, public_slug, visible_blocks, updated_at)
                   VALUES (%s, %s, %s, %s::jsonb, NOW())
                   ON CONFLICT (user_id) DO UPDATE SET
                     public_enabled = COALESCE(EXCLUDED.public_enabled, user_stats_settings.public_enabled),
                     public_slug = CASE WHEN EXCLUDED.public_enabled THEN COALESCE(user_stats_settings.public_slug, EXCLUDED.public_slug) ELSE user_stats_settings.public_slug END,
                     visible_blocks = COALESCE(EXCLUDED.visible_blocks::jsonb, user_stats_settings.visible_blocks),
                     updated_at = NOW()""",
                (user_id, public_enabled if public_enabled is not None else False, slug, json.dumps(vb))
            )
        else:
            curr_slug = row.get('public_slug') if isinstance(row, dict) else row[1]
            curr_enabled = row.get('public_enabled') if isinstance(row, dict) else row[0]
            vb = visible_blocks if visible_blocks is not None else (row.get('visible_blocks') if isinstance(row, dict) else row[2])
            if isinstance(vb, str):
                vb = json.loads(vb) if vb else {}
            slug = curr_slug
            if public_enabled and not curr_slug:
                cur.execute(
                    "SELECT username FROM stats WHERE chat_id = %s AND user_id = %s AND username IS NOT NULL AND username != '' ORDER BY id DESC LIMIT 1",
                    (user_id, user_id)
                )
                un_row = cur.fetchone()
                un = (un_row.get('username') if isinstance(un_row, dict) else un_row[0]) if un_row else None
                slug = (un or '').lstrip('@').lower().replace(' ', '_')[:32] if un else f'user_{user_id}'
                cur.execute("SELECT 1 FROM user_stats_settings WHERE public_slug = %s AND user_id != %s", (slug, user_id))
                if cur.fetchone():
                    slug = f'user_{user_id}'
            cur.execute(
                """UPDATE user_stats_settings SET
                     public_enabled = COALESCE(%s, public_enabled),
                     public_slug = CASE WHEN %s THEN COALESCE(public_slug, %s) ELSE public_slug END,
                     visible_blocks = COALESCE(%s::jsonb, visible_blocks),
                     updated_at = NOW()
                   WHERE user_id = %s""",
                (public_enabled, bool(public_enabled), slug, json.dumps(vb), user_id)
            )
        get_db_connection().commit()
    return get_user_stats_settings(user_id)


def get_public_personal_stats(slug, month, year):
    """Публичная личная статистика по slug. Без авторизации."""
    cur = get_db_cursor()
    with db_lock:
        cur.execute(
            "SELECT user_id, public_enabled, visible_blocks FROM user_stats_settings WHERE public_slug = %s",
            (slug,)
        )
        row = cur.fetchone()
    if not row:
        return None, 'User not found'
    user_id = row.get('user_id') if isinstance(row, dict) else row[0]
    public_enabled = row.get('public_enabled') if isinstance(row, dict) else row[1]
    if not public_enabled:
        return None, 'Stats are private'
    vb = row.get('visible_blocks') if isinstance(row, dict) else row[2]
    if isinstance(vb, str):
        import json
        vb = json.loads(vb) if vb else {}
    vb = vb or {
        'summary': True, 'top_films': True, 'rating_breakdown': True,
        'cinema': True, 'platforms': True, 'watched_list': True
    }
    data = get_personal_stats(user_id, month, year)
    # Фильтруем по visible_blocks
    if not vb.get('summary', True):
        data.pop('summary', None)
    if not vb.get('top_films', True):
        data['top_films'] = []
    if not vb.get('rating_breakdown', True):
        data['rating_breakdown'] = {str(i): 0 for i in range(1, 11)}
    if not vb.get('cinema', True):
        data['cinema'] = []
    if not vb.get('platforms', True):
        data['platforms'] = []
    if not vb.get('watched_list', True):
        data['watched'] = []
    # Имя пользователя
    with db_lock:
        cur.execute("SELECT name FROM site_sessions WHERE chat_id = %s ORDER BY created_at DESC LIMIT 1", (user_id,))
        srow = cur.fetchone()
        cur.execute("SELECT username FROM stats WHERE chat_id = %s AND user_id = %s AND username IS NOT NULL AND username != '' ORDER BY id DESC LIMIT 1", (user_id, user_id))
        urow = cur.fetchone()
    name = (srow.get('name') if isinstance(srow, dict) else srow[0]) if srow else None
    username = (urow.get('username') if isinstance(urow, dict) else urow[0]) if urow else slug
    if username and not username.startswith('@'):
        username = '@' + username
    data['user'] = {'name': name or slug, 'username': username or slug}
    data['success'] = True
    return data, None


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
