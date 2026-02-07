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


# Список вечных личных ачивок (id, icon, name, description, rarity, условие)
PERSONAL_ACHIEVEMENTS_DEF = [
    ('first_watch', '🎬', 'Первый кадр', 'Первый фильм в MP', 'common', lambda p: p.get('total_films_alltime', 0) >= 1),
    ('club_50', '🎞️', 'Полтинник', '50 просмотренных фильмов', 'common', lambda p: p.get('total_films_alltime', 0) >= 50),
    ('club_100', '💯', 'Сотня', '100 просмотренных фильмов', 'rare', lambda p: p.get('total_films_alltime', 0) >= 100),
    ('club_250', '🏆', 'Четверть тысячи', '250 фильмов', 'epic', lambda p: p.get('total_films_alltime', 0) >= 250),
    ('club_500', '💎', 'Пятьсот', '500 фильмов', 'legendary', lambda p: p.get('total_films_alltime', 0) >= 500),
    ('critic_100', '⭐', 'Критик', '100 оценок', 'common', lambda p: p.get('total_ratings_alltime', 0) >= 100),
    ('critic_500', '🎓', 'Эксперт', '500 оценок', 'epic', lambda p: p.get('total_ratings_alltime', 0) >= 500),
    ('cinema_1', '🎟️', 'Первый сеанс', 'Первый поход в кино через Movie Planner', 'common', lambda p: p.get('total_cinema_alltime', 0) >= 1),
    ('cinema_10', '🎥', 'Кинозритель', '10 походов в кино', 'common', lambda p: p.get('total_cinema_alltime', 0) >= 10),
    ('cinema_25', '🎬', 'Завсегдатай залов', '25 походов в кино', 'rare', lambda p: p.get('total_cinema_alltime', 0) >= 25),
    ('cinema_50', '🍿', 'Синефил', '50 походов в кино', 'epic', lambda p: p.get('total_cinema_alltime', 0) >= 50),
    ('cinema_100', '🏟️', 'Кинофанат', '100 походов в кино', 'legendary', lambda p: p.get('total_cinema_alltime', 0) >= 100),
    ('series_5', '📺', 'Сериаломан', '5 завершённых сериалов', 'common', lambda p: p.get('completed_series_alltime', 0) >= 5),
    ('series_500ep', '🔥', '500 серий', '500 отмеченных серий', 'rare', lambda p: p.get('total_episodes_alltime', 0) >= 500),
    ('genres_10', '🌈', 'Всеядный', '10+ жанров', 'rare', lambda p: p.get('unique_genres_alltime', 0) >= 10),
    ('year_streak', '📅', 'Годовой стрик', '12 месяцев подряд с активностью', 'epic', lambda p: p.get('year_streak', False)),
    ('oldtimer', '🏠', 'Старожил', '1+ год в MP', 'rare', lambda p: p.get('months_since_first_action', 0) >= 12),
    ('mvp_legend', '👑', 'Легенда', '6 раз «Киноман месяца» в группе', 'legendary', lambda p: p.get('mvp_count', 0) >= 6),
]


def _get_user_profile_and_achievements(user_id):
    """Вычисляет user_profile (alltime) и список вечных ачивок. Без хранения в БД."""
    cur = get_db_cursor()
    chat_id = user_id  # личный чат

    profile = {
        'username': None,
        'first_name': None,
        'member_since': None,
        'total_films_alltime': 0,
        'total_series_alltime': 0,
        'avg_rating_alltime': None,
        'total_ratings_alltime': 0,
        'total_cinema_alltime': 0,
        'completed_series_alltime': 0,
        'total_episodes_alltime': 0,
        'unique_genres_alltime': 0,
        'year_streak': False,
        'months_since_first_action': 0,
        'mvp_count': 0,
    }

    with db_lock:
        cur.execute("SELECT name FROM site_sessions WHERE chat_id = %s ORDER BY created_at ASC LIMIT 1", (chat_id,))
        row = cur.fetchone()
        if row:
            profile['first_name'] = row.get('name') if isinstance(row, dict) else row[0]
        cur.execute("SELECT username FROM stats WHERE chat_id = %s AND user_id = %s AND username IS NOT NULL ORDER BY id DESC LIMIT 1", (chat_id, user_id))
        row = cur.fetchone()
        if row:
            un = row.get('username') if isinstance(row, dict) else row[0]
            profile['username'] = ('@' + un) if un and not str(un).startswith('@') else un

        cur.execute("""
            SELECT COUNT(DISTINCT wm.film_id) FROM watched_movies wm
            JOIN movies m ON m.id = wm.film_id AND m.chat_id = wm.chat_id
            WHERE wm.chat_id = %s AND wm.user_id = %s AND (m.is_series IS NULL OR m.is_series = 0)
        """, (chat_id, user_id))
        r = cur.fetchone()
        films_wm = (r.get('count') if isinstance(r, dict) else r[0]) or 0
        cur.execute("""
            SELECT COUNT(DISTINCT r.film_id) FROM ratings r
            JOIN movies m ON m.id = r.film_id AND m.chat_id = r.chat_id
            WHERE r.chat_id = %s AND r.user_id = %s AND (m.is_series IS NULL OR m.is_series = 0)
        """, (chat_id, user_id))
        r = cur.fetchone()
        films_rat = (r.get('count') if isinstance(r, dict) else r[0]) or 0
        cur.execute("""
            SELECT COUNT(*) FROM ratings WHERE chat_id = %s AND user_id = %s
        """, (chat_id, user_id))
        r = cur.fetchone()
        profile['total_ratings_alltime'] = (r.get('count') if isinstance(r, dict) else r[0]) or 0
        cur.execute("""
            SELECT COUNT(*) FROM cinema_screenings WHERE chat_id = %s AND user_id = %s
        """, (chat_id, user_id))
        r = cur.fetchone()
        profile['total_cinema_alltime'] = (r.get('count') if isinstance(r, dict) else r[0]) or 0
        cur.execute("""
            SELECT COUNT(DISTINCT st.film_id) FROM series_tracking st
            JOIN movies m ON m.id = st.film_id AND m.chat_id = st.chat_id
            WHERE st.chat_id = %s AND st.user_id = %s AND st.watched = TRUE AND m.is_series != 0
        """, (chat_id, user_id))
        r = cur.fetchone()
        profile['completed_series_alltime'] = (r.get('count') if isinstance(r, dict) else r[0]) or 0
        cur.execute("""
            SELECT COUNT(*) FROM series_tracking st
            JOIN movies m ON m.id = st.film_id AND m.chat_id = st.chat_id
            WHERE st.chat_id = %s AND st.user_id = %s AND st.watched = TRUE
        """, (chat_id, user_id))
        r = cur.fetchone()
        profile['total_episodes_alltime'] = (r.get('count') if isinstance(r, dict) else r[0]) or 0
        cur.execute("""
            SELECT m.genres FROM ratings r
            JOIN movies m ON m.id = r.film_id AND m.chat_id = r.chat_id
            WHERE r.chat_id = %s AND r.user_id = %s AND m.genres IS NOT NULL AND m.genres != ''
        """, (chat_id, user_id))
        genres_set = set()
        for row in cur.fetchall():
            g = row.get('genres') if isinstance(row, dict) else row[0]
            if g:
                for part in str(g).replace(',', ' ').split():
                    if len(part) > 1:
                        genres_set.add(part.strip())
        profile['unique_genres_alltime'] = len(genres_set)
        cur.execute("""
            SELECT COUNT(DISTINCT m.id) FROM movies m
            WHERE m.chat_id = %s AND (m.is_series IS NULL OR m.is_series = 0)
              AND (EXISTS (SELECT 1 FROM watched_movies wm WHERE wm.chat_id = m.chat_id AND wm.film_id = m.id AND wm.user_id = %s)
                   OR EXISTS (SELECT 1 FROM ratings r WHERE r.chat_id = m.chat_id AND r.film_id = m.id AND r.user_id = %s))
        """, (chat_id, user_id, user_id))
        r = cur.fetchone()
        profile['total_films_alltime'] = (r.get('count') if isinstance(r, dict) else r[0]) or 0
        cur.execute("""
            SELECT COUNT(DISTINCT st.film_id) FROM series_tracking st
            WHERE st.chat_id = %s AND st.user_id = %s AND st.watched = TRUE
        """, (chat_id, user_id))
        r = cur.fetchone()
        profile['total_series_alltime'] = (r.get('count') if isinstance(r, dict) else r[0]) or 0
        cur.execute("""
            SELECT AVG(rating)::numeric(4,2) FROM ratings WHERE chat_id = %s AND user_id = %s
        """, (chat_id, user_id))
        r = cur.fetchone()
        avg = r.get('avg') if isinstance(r, dict) else (r[0] if r and len(r) > 0 else None)
        profile['avg_rating_alltime'] = round(float(avg), 1) if avg is not None else None
        first_ts = None
        for q in [
            "SELECT MIN(watched_at) AS ts FROM watched_movies WHERE chat_id = %s AND user_id = %s",
            "SELECT MIN(rated_at) AS ts FROM ratings WHERE chat_id = %s AND user_id = %s",
        ]:
            cur.execute(q, (chat_id, user_id))
            r = cur.fetchone()
            val = (r.get('ts') if isinstance(r, dict) else (r[0] if r and len(r) > 0 else None)) if r else None
            if val:
                dt = _ensure_tz(val)
                if first_ts is None or (dt and dt < first_ts):
                    first_ts = dt
        if first_ts:
            profile['member_since'] = first_ts.strftime('%Y-%m-%d')
            now = datetime.now(pytz.UTC)
            months = (now.year - first_ts.year) * 12 + (now.month - first_ts.month)
            profile['months_since_first_action'] = max(0, months)
        mvp_count = 0
        cur.execute("SELECT chat_id FROM group_stats_settings WHERE 1=0")
        profile['mvp_count'] = mvp_count
        year_streak = False
        if first_ts:
            cur.execute("""
                SELECT DISTINCT date_trunc('month', t.dt)::date as m
                FROM (
                    SELECT watched_at as dt FROM watched_movies WHERE chat_id = %s AND user_id = %s AND watched_at IS NOT NULL
                    UNION ALL
                    SELECT rated_at as dt FROM ratings WHERE chat_id = %s AND user_id = %s AND rated_at IS NOT NULL
                ) t
                ORDER BY m
            """, (chat_id, user_id, chat_id, user_id))
            months_with_activity = []
            for row in cur.fetchall():
                m = row.get('m') if isinstance(row, dict) else row[0]
                if m and hasattr(m, 'year'):
                    months_with_activity.append((m.year, m.month))
            months_set = set(months_with_activity)
            if len(months_set) >= 12:
                for y, mo in months_set:
                    needed = []
                    cy, cmo = y, mo
                    for _ in range(12):
                        needed.append((cy, cmo))
                        if cmo == 12:
                            cy, cmo = cy + 1, 1
                        else:
                            cmo += 1
                    if all(m in months_set for m in needed):
                        year_streak = True
                        break
        profile['year_streak'] = year_streak

    user_profile_out = {
        'username': profile.get('username') or f'user_{user_id}',
        'first_name': profile.get('first_name') or profile.get('username') or f'user_{user_id}',
        'member_since': profile.get('member_since'),
        'total_films_alltime': profile.get('total_films_alltime', 0),
        'total_series_alltime': profile.get('total_series_alltime', 0),
        'avg_rating_alltime': profile.get('avg_rating_alltime'),
    }

    achievements = []
    for ach_id, icon, name, desc, rarity, check in PERSONAL_ACHIEVEMENTS_DEF:
        earned = check(profile)
        target_val = None
        current_val = None
        if ach_id == 'club_250':
            target_val, current_val = 250, profile.get('total_films_alltime', 0)
        elif ach_id == 'club_500':
            target_val, current_val = 500, profile.get('total_films_alltime', 0)
        elif ach_id == 'critic_500':
            target_val, current_val = 500, profile.get('total_ratings_alltime', 0)
        elif ach_id == 'cinema_50':
            target_val, current_val = 50, profile.get('total_cinema_alltime', 0)
        elif ach_id == 'cinema_100':
            target_val, current_val = 100, profile.get('total_cinema_alltime', 0)
        elif ach_id == 'series_500ep':
            target_val, current_val = 500, profile.get('total_episodes_alltime', 0)
        elif ach_id == 'year_streak':
            target_val, current_val = 12, 12 if profile.get('year_streak') else profile.get('months_since_first_action', 0)
        elif ach_id == 'oldtimer':
            target_val, current_val = 12, profile.get('months_since_first_action', 0)
        elif ach_id == 'mvp_legend':
            target_val, current_val = 6, profile.get('mvp_count', 0)
        progress = None
        if not earned and target_val is not None and current_val is not None:
            progress = {'current': min(current_val, target_val), 'target': target_val}
        earned_date = profile.get('member_since') if earned and ach_id == 'first_watch' else None
        achievements.append({
            'id': ach_id,
            'icon': icon,
            'name': name,
            'description': desc,
            'rarity': rarity,
            'earned': earned,
            'earned_date': earned_date,
            'progress': progress,
        })

    earned_count = sum(1 for a in achievements if a['earned'])
    achievements.append({
        'id': 'collector',
        'icon': '🎖️',
        'name': 'Коллекционер',
        'description': '10 ачивок получено',
        'rarity': 'rare',
        'earned': earned_count >= 10,
        'earned_date': None,
        'progress': None if earned_count >= 10 else {'current': earned_count, 'target': 10},
    })

    return user_profile_out, achievements


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

        # Серии из series_tracking (year, online_link для watched_list)
        cur.execute("""
            SELECT st.film_id, st.watched_date, m.kp_id, m.title, m.year, m.is_series, m.online_link
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
        is_series = r.get('is_series') if isinstance(r, dict) else r[5]
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

    # Рейтинги = просмотрено (оценка подразумевает просмотр). Добавляем фильмы и сериалы из ratings_in_month.
    for r in ratings_in_month:
        fid = r.get('film_id') if isinstance(r, dict) else r[0]
        is_series = r.get('is_series') if isinstance(r, dict) else r[7]
        if is_series:
            series_watched.add(fid)
        else:
            films_watched.add(fid)

    # Cinema: из cinema_screenings (планы удаляются, записи остаются) + plans для актуальных планов
    cinema_rows = []
    with db_lock:
        cur.execute("""
            SELECT cs.film_id, cs.screening_date, m.kp_id, m.title, m.year
            FROM cinema_screenings cs
            JOIN movies m ON m.id = cs.film_id AND m.chat_id = cs.chat_id
            WHERE cs.chat_id = %s AND cs.user_id = %s
              AND cs.screening_date >= %s AND cs.screening_date < %s
        """, (chat_id, user_id, start_ts.date(), end_ts.date()))
        cinema_rows = list(cur.fetchall())
        # Дополняем планами (если ещё не удалены; в личке chat_id=user_id)
        cur.execute("""
            SELECT p.film_id, p.plan_datetime::date, m.kp_id, m.title, m.year
            FROM plans p
            JOIN movies m ON m.id = p.film_id AND m.chat_id = p.chat_id
            WHERE p.chat_id = %s AND p.plan_type = 'cinema' AND (p.user_id = %s OR p.user_id IS NULL)
              AND p.plan_datetime >= %s AND p.plan_datetime < %s
              AND NOT EXISTS (
                SELECT 1 FROM cinema_screenings cs
                WHERE cs.chat_id = p.chat_id AND cs.user_id = %s AND cs.film_id = p.film_id
              )
        """, (chat_id, user_id, start_ts, end_ts, user_id))
        for row in cur.fetchall():
            cinema_rows.append(row)

    cinema_film_ids = {r.get('film_id') if isinstance(r, dict) else r[0] for r in cinema_rows}
    # Собираем watched для вывода (из watched_movies + series_tracking + ratings) — всё, что в summary
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
                'rating': rating, 'is_cinema': fid in cinema_film_ids,
                'online_link': online_link
            })

    # Добавляем сериалы из series_tracking (по одному на film_id, с последней датой в месяце)
    series_by_fid = {}  # film_id -> row (с макс watched_date)
    for r in series_rows:
        fid = r.get('film_id') if isinstance(r, dict) else r[0]
        wd = r.get('watched_date') if isinstance(r, dict) else r[1]
        if fid not in series_by_fid:
            series_by_fid[fid] = r
        else:
            prev = series_by_fid[fid]
            pw = prev.get('watched_date') if isinstance(prev, dict) else prev[1]
            if wd and pw and (not hasattr(wd, 'strftime') or not hasattr(pw, 'strftime') or wd > pw):
                series_by_fid[fid] = r
    for fid, r in series_by_fid.items():
        key = (fid, 'st')
        if key in seen:
            continue
        seen.add(key)
        kp_id = r.get('kp_id') if isinstance(r, dict) else r[2]
        title = r.get('title') if isinstance(r, dict) else r[3]
        year = r.get('year') if isinstance(r, dict) else r[4]
        is_series = r.get('is_series') if isinstance(r, dict) else r[5]
        wd = r.get('watched_date') if isinstance(r, dict) else r[1]
        online_link = r.get('online_link') if isinstance(r, dict) else (r[6] if len(r) > 6 else None)
        date_str = wd.strftime('%Y-%m-%d') if hasattr(wd, 'strftime') else (str(wd)[:10] if wd else '')
        rating = next((x.get('rating') for x in ratings_in_month if (x.get('film_id') == fid)), None)
        watched_list.append({
            'film_id': fid, 'kp_id': kp_id, 'title': title, 'year': year,
            'type': 'series' if is_series else 'film',
            'date': date_str, 'rating': rating, 'is_cinema': fid in cinema_film_ids,
            'online_link': online_link
        })

    # Добавляем из ratings (оценены за месяц, но не в watched_movies/series_tracking)
    for r in ratings_in_month:
        fid = r.get('film_id') if isinstance(r, dict) else r[0]
        if (fid, 'wm') in seen or (fid, 'st') in seen:
            continue
        kp_id = r.get('kp_id') if isinstance(r, dict) else r[4]
        title = r.get('title') if isinstance(r, dict) else r[5]
        year = r.get('year') if isinstance(r, dict) else r[6]
        is_series = r.get('is_series') if isinstance(r, dict) else r[7]
        rt = r.get('rating') if isinstance(r, dict) else r[1]
        rt_at = r.get('rated_at') if isinstance(r, dict) else (r[2] if len(r) > 2 else None)
        date_str = None
        if rt_at:
            dt = _ensure_tz(rt_at)
            if dt and hasattr(dt, 'strftime'):
                date_str = dt.strftime('%Y-%m-%d')
        if not date_str:
            date_str = datetime.now(pytz.UTC).strftime('%Y-%m-%d')
        seen.add((fid, 'r'))
        watched_list.append({
            'film_id': fid, 'kp_id': kp_id, 'title': title, 'year': year,
            'type': 'series' if is_series else 'film',
            'date': date_str, 'rating': rt, 'is_cinema': fid in cinema_film_ids,
            'online_link': None
        })

    # Добавляем из cinema (походы в кино, если ещё не в списке)
    for r in cinema_rows:
        fid = r.get('film_id') if isinstance(r, dict) else r[0]
        if (fid, 'wm') in seen or (fid, 'st') in seen or (fid, 'r') in seen:
            continue
        kp_id = r.get('kp_id') if isinstance(r, dict) else r[2]
        title = r.get('title') if isinstance(r, dict) else r[3]
        year = r.get('year') if isinstance(r, dict) else (r[4] if len(r) > 4 else None)
        dt = r.get('screening_date') or r.get('plan_datetime') if isinstance(r, dict) else r[1]
        date_str = dt.strftime('%Y-%m-%d') if hasattr(dt, 'strftime') else (str(dt)[:10] if dt else '')
        rating = next((x.get('rating') for x in ratings_in_month if (x.get('film_id') == fid)), None)
        seen.add((fid, 'c'))
        watched_list.append({
            'film_id': fid, 'kp_id': kp_id, 'title': title, 'year': year,
            'type': 'film',
            'date': date_str, 'rating': rating, 'is_cinema': True,
            'online_link': None
        })

    # Страховка: всё из films_watched | series_watched должно быть в watched_list
    watched_fids = {w.get('film_id') for w in watched_list}
    all_expected = films_watched | series_watched
    missing = all_expected - watched_fids
    ratings_by_fid = {r.get('film_id') if isinstance(r, dict) else r[0]: r for r in ratings_all}
    for fid in missing:
        r = ratings_by_fid.get(fid)
        if r:
            kp_id = r.get('kp_id') if isinstance(r, dict) else r[4]
            title = r.get('title') if isinstance(r, dict) else r[5]
            year = r.get('year') if isinstance(r, dict) else r[6]
            is_series = r.get('is_series') if isinstance(r, dict) else r[7]
            rt = r.get('rating') if isinstance(r, dict) else r[1]
            rt_at = r.get('rated_at') if isinstance(r, dict) else (r[2] if len(r) > 2 else None)
            date_str = start_ts.strftime('%Y-%m-%d')
            if rt_at:
                dt = _ensure_tz(rt_at)
                if dt and hasattr(dt, 'strftime'):
                    date_str = dt.strftime('%Y-%m-%d')
            watched_list.append({
                'film_id': fid, 'kp_id': kp_id, 'title': title, 'year': year,
                'type': 'series' if is_series else 'film',
                'date': date_str, 'rating': rt, 'is_cinema': fid in cinema_film_ids,
                'online_link': None
            })

    watched_list.sort(key=lambda w: (w.get('date') or '', w.get('title') or ''))

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

    # total_watched = films + episodes (каждый фильм и каждая серия считаются как 1 просмотр)
    total_views = len(films_watched) + episodes_count
    out = {
        'period': {'month': month, 'year': year, 'label': f'{MONTH_NAMES_RU[month - 1]} {year}'},
        'summary': {
            'films_watched': len(films_watched),
            'series_watched': len(series_watched),
            'episodes_watched': episodes_count,
            'cinema_visits': len(cinema_rows),
            'total_watched': total_views,
            'avg_rating': avg_rating
        },
        'top_films': top_films,
        'cinema': cinema_list,
        'platforms': platforms,
        'watched': watched_list,
        'rating_breakdown': rating_breakdown
    }
    profile, achievements = _get_user_profile_and_achievements(user_id)
    if profile:
        out['user_profile'] = profile
    if achievements:
        out['achievements'] = achievements
    return out


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
            SELECT wm.film_id, wm.user_id, m.kp_id, m.title, m.year, m.is_series, wm.watched_at, m.genres
            FROM watched_movies wm
            JOIN movies m ON m.id = wm.film_id AND m.chat_id = wm.chat_id
            WHERE wm.chat_id = %s AND wm.watched_at >= %s AND wm.watched_at < %s
        """, (chat_id, start_ts, end_ts))
        wm_rows = cur.fetchall()

    # series_tracking за месяц
    with db_lock:
        cur.execute("""
            SELECT st.film_id, st.user_id, m.kp_id, m.title, m.year, m.is_series, st.watched_date, m.genres
            FROM series_tracking st
            JOIN movies m ON m.id = st.film_id AND m.chat_id = st.chat_id
            WHERE st.chat_id = %s AND st.watched = TRUE
              AND st.watched_date >= %s AND st.watched_date < %s
        """, (chat_id, start_ts, end_ts))
        st_rows = cur.fetchall()

    # Cinema: cinema_screenings (постоянные) + plans (если ещё не удалены)
    cinema_rows = []
    with db_lock:
        cur.execute("""
            SELECT cs.film_id, cs.user_id, cs.screening_date, m.kp_id, m.title, m.year
            FROM cinema_screenings cs
            JOIN movies m ON m.id = cs.film_id AND m.chat_id = cs.chat_id
            WHERE cs.chat_id = %s AND cs.screening_date >= %s AND cs.screening_date < %s
        """, (chat_id, start_ts.date(), end_ts.date()))
        cinema_rows = list(cur.fetchall())
        cur.execute("""
            SELECT p.film_id, p.user_id, p.plan_datetime::date, m.kp_id, m.title, m.year
            FROM plans p
            JOIN movies m ON m.id = p.film_id AND m.chat_id = p.chat_id
            WHERE p.chat_id = %s AND p.plan_type = 'cinema'
              AND p.plan_datetime >= %s AND p.plan_datetime < %s
              AND NOT EXISTS (
                SELECT 1 FROM cinema_screenings cs
                WHERE cs.chat_id = p.chat_id AND cs.user_id = p.user_id AND cs.film_id = p.film_id
              )
        """, (chat_id, start_ts, end_ts))
        for row in cur.fetchall():
            cinema_rows.append(row)

    # ratings за месяц (rated_at если есть). ТОЛЬКО группа: r.chat_id = группа, фильмы из базы группы.
    with db_lock:
        cur.execute("""
            SELECT r.rating, r.user_id, r.film_id, r.rated_at, m.kp_id, m.title, m.year, m.genres, m.is_series
            FROM ratings r
            JOIN movies m ON m.id = r.film_id AND m.chat_id = r.chat_id
            WHERE r.chat_id = %s
        """, (chat_id,))
        ratings_all = cur.fetchall()

    # Фильтруем ratings строго по месяцу (rated_at). Без даты — не включаем (данные по группе только за месяц)
    ratings_in_month = []
    for r in ratings_all:
        rt_at = r.get('rated_at') if isinstance(r, dict) else (r[3] if len(r) > 3 else None)
        if rt_at is not None:
            dt = _ensure_tz(rt_at)
            if dt and start_ts <= dt < end_ts:
                ratings_in_month.append(r)

    # Агрегация: group_films (уникальные фильмы), group_series, group_episodes
    # Оценка = просмотр: считаем ratings_in_month как просмотры
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
    # Оценка = просмотр: каждая оценка за месяц считается просмотром
    for r in ratings_in_month:
        fid = r.get('film_id') if isinstance(r, dict) else r[2]
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        is_series = r.get('is_series') if isinstance(r, dict) else (r[8] if len(r) > 8 else False)
        if is_series:
            series_watched.add(fid)
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

    # Все ачивки по spec
    def _holder(cond, uid): return uid if cond else None
    uid_series = defaultdict(int)
    for r in st_rows:
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        uid_series[uid] += 1
    uid_genres = defaultdict(set)
    for r in wm_rows + st_rows:
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        g = r.get('genres') if isinstance(r, dict) else (r[7] if len(r) > 7 else None)
        if g:
            for gen in (g or '').split(','):
                gen = gen.strip()
                if gen:
                    uid_genres[uid].add(gen)
    # Киноман, Оценщик — уже есть mvp_uid. Остальные — ищем лучшего по условию
    cinephile_uid = max(uid_watched.items(), key=lambda x: x[1])[0] if uid_watched and max(uid_watched.values()) >= 10 else None
    rater_uid = max(uid_ratings.items(), key=lambda x: len(x[1]))[0] if uid_ratings and max(len(r) for r in uid_ratings.values()) >= 15 else None
    cinema_uid = max(uid_cinema.items(), key=lambda x: x[1])[0] if uid_cinema and max(uid_cinema.values()) >= 3 else None
    series_uid = max(uid_series.items(), key=lambda x: x[1])[0] if uid_series and max(uid_series.values()) >= 20 else None
    strict_uid = None  # средняя < 7 при ≥5 оценках
    for uid, rats in uid_ratings.items():
        if len(rats) >= 5 and sum(rats) / len(rats) < 7:
            strict_uid = uid
            break
    generous_uid = None  # средняя > 8 при ≥5 оценках
    for uid, rats in uid_ratings.items():
        if len(rats) >= 5 and sum(rats) / len(rats) > 8:
            generous_uid = uid
            break
    polyglot_uid = max(uid_genres.items(), key=lambda x: len(x[1]))[0] if uid_genres and max(len(g) for g in uid_genres.values()) >= 5 else None
    # Первооткрыватель: добавил фильм в базу группы, все оценили ≥8 (за месяц)
    discoverer_uid = None
    film_ratings_for_discoverer = defaultdict(list)
    for r in ratings_in_month:
        fid = r.get('film_id') if isinstance(r, dict) else r[2]
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        rt = r.get('rating') if isinstance(r, dict) else r[0]
        if fid and rt:
            film_ratings_for_discoverer[fid].append({'user_id': uid, 'rating': rt})
    for fid, rats in film_ratings_for_discoverer.items():
        if len(rats) >= 2 and all(r['rating'] >= 8 for r in rats):
            with db_lock:
                cur.execute("SELECT added_by FROM movies WHERE id = %s AND chat_id = %s", (fid, chat_id))
                row = cur.fetchone()
            if row:
                ab = row.get('added_by') if isinstance(row, dict) else row[0]
                if ab:
                    discoverer_uid = ab
                    break
    achievements = [
        {'id': 'cinephile', 'icon': '🎬', 'name': 'Киноман', 'description': 'Посмотрел 10+ фильмов за месяц',
         'holder_user_id': cinephile_uid, 'earned': cinephile_uid is not None},
        {'id': 'strict_critic', 'icon': '⭐', 'name': 'Строгий критик', 'description': 'Средняя оценка ниже 7',
         'holder_user_id': strict_uid, 'earned': strict_uid is not None},
        {'id': 'frequent_goer', 'icon': '🍿', 'name': 'Завсегдатай', 'description': '3+ похода в кино',
         'holder_user_id': cinema_uid, 'earned': cinema_uid is not None},
        {'id': 'generous', 'icon': '💕', 'name': 'Добряк', 'description': 'Средняя оценка выше 8',
         'holder_user_id': generous_uid, 'earned': generous_uid is not None},
        {'id': 'binge_watcher', 'icon': '🔥', 'name': 'Серийный убийца', 'description': '20+ серий за месяц',
         'holder_user_id': series_uid, 'earned': series_uid is not None},
        {'id': 'rater', 'icon': '📊', 'name': 'Оценщик', 'description': '15+ оценок за месяц',
         'holder_user_id': rater_uid, 'earned': rater_uid is not None},
        {'id': 'polyglot', 'icon': '🌍', 'name': 'Полиглот', 'description': '5+ жанров за месяц',
         'holder_user_id': polyglot_uid, 'earned': polyglot_uid is not None},
        {'id': 'discoverer', 'icon': '👀', 'name': 'Первооткрыватель', 'description': 'Нашёл фильм, который все оценили 8+',
         'holder_user_id': discoverer_uid, 'earned': discoverer_uid is not None},
    ]

    # top_films по средней оценке (фильмы с мин. 2 оценками)
    film_ratings = defaultdict(list)
    for r in ratings_in_month:
        fid = r.get('film_id') if isinstance(r, dict) else r[2]
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        rt = r.get('rating') if isinstance(r, dict) else r[0]
        kp_id = r.get('kp_id') if isinstance(r, dict) else r[4]
        title = r.get('title') if isinstance(r, dict) else r[5]
        year = r.get('year') if isinstance(r, dict) else r[6]
        genres = r.get('genres') if isinstance(r, dict) else r[7]
        if fid and rt:
            film_ratings[fid].append({'user_id': uid, 'rating': rt, 'kp_id': kp_id, 'title': title, 'year': year, 'genre': genres or ''})
    top_films = []
    for fid, rats in film_ratings.items():
        if len(rats) < 2:
            continue
        avg = round(sum(r['rating'] for r in rats) / len(rats), 1)
        r0 = rats[0]
        top_films.append({
            'film_id': fid, 'kp_id': r0.get('kp_id'), 'title': r0.get('title'), 'year': r0.get('year'),
            'genre': r0.get('genre', ''), 'avg_rating': avg,
            'rated_by': [{'user_id': r['user_id'], 'rating': r['rating']} for r in rats]
        })
    top_films.sort(key=lambda x: (-(x.get('avg_rating') or 0), -(len(x.get('rated_by') or [])), x.get('title') or ''))
    top_films = top_films[:10]

    # Кино: (film_id, user_id) для бейджа
    cinema_film_user = {(r.get('film_id') if isinstance(r, dict) else r[0], r.get('user_id') if isinstance(r, dict) else r[1]) for r in cinema_rows}

    # Watched list для группы (всё просмотренное за месяц)
    watched_list = []
    seen = set()
    def add_watched(fid, kp_id, title, year_val, is_series, date_val, uid):
        key = (fid, uid, date_val)
        if key in seen:
            return
        seen.add(key)
        rating = next((r.get('rating') for r in ratings_in_month if r.get('film_id') == fid and r.get('user_id') == uid), None)
        is_cinema = (fid, uid) in cinema_film_user
        watched_list.append({
            'film_id': fid, 'kp_id': kp_id, 'title': title, 'year': year_val,
            'type': 'series' if is_series else 'film',
            'date': date_val.strftime('%Y-%m-%d') if hasattr(date_val, 'strftime') else str(date_val)[:10],
            'rating': rating, 'user_id': uid, 'is_cinema': is_cinema
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
    # Добавляем в watched фильмы из оценок и походов в кино (если их ещё нет)
    for r in ratings_in_month:
        fid = r.get('film_id') if isinstance(r, dict) else r[2]
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        kp_id = r.get('kp_id') if isinstance(r, dict) else r[4]
        title = r.get('title') if isinstance(r, dict) else r[5]
        year = r.get('year') if isinstance(r, dict) else (r[6] if len(r) > 6 else None)
        is_series = r.get('is_series') if isinstance(r, dict) else (r[8] if len(r) > 8 else False)
        dt = r.get('rated_at') if isinstance(r, dict) else r[3]
        if dt:
            add_watched(fid, kp_id, title, year_val=year, is_series=bool(is_series), date_val=dt, uid=uid)
    for r in cinema_rows:
        fid = r.get('film_id') if isinstance(r, dict) else r[0]
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        kp_id = r.get('kp_id') if isinstance(r, dict) else r[3]
        title = r.get('title') if isinstance(r, dict) else r[4]
        year = r.get('year') if isinstance(r, dict) else (r[5] if len(r) > 5 else None)
        dt = r.get('screening_date') or r.get('plan_datetime') if isinstance(r, dict) else r[2]
        add_watched(fid, kp_id, title, year_val=year, is_series=False, date_val=dt, uid=uid)
    watched_list.sort(key=lambda x: (x.get('date') or '', x.get('title') or ''))

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

    # Controversial: фильмы с ≥3 оценками, max spread
    controversial = []
    for fid, rats in film_ratings.items():
        if len(rats) < 3:
            continue
        ratings_vals = [r['rating'] for r in rats]
        spread = max(ratings_vals) - min(ratings_vals)
        r0 = rats[0]
        controversial.append({
            'film_id': fid, 'kp_id': r0.get('kp_id'), 'title': r0.get('title'), 'year': r0.get('year'),
            'ratings': [{'user_id': r['user_id'], 'rating': r['rating']} for r in rats],
            'spread': spread, 'avg_rating': round(sum(ratings_vals) / len(ratings_vals), 1)
        })
    controversial.sort(key=lambda x: -x['spread'])
    controversial = controversial[:5]

    # Compatibility: пары участников, общие фильмы, MAE -> pct
    compatibility = []
    pair_common = defaultdict(lambda: [])  # (uid1, uid2) -> [(r1, r2), ...]
    for fid, rats in film_ratings.items():
        uids = list({r['user_id'] for r in rats})
        for i, u1 in enumerate(uids):
            for u2 in uids[i+1:]:
                r1 = next((r['rating'] for r in rats if r['user_id'] == u1), None)
                r2 = next((r['rating'] for r in rats if r['user_id'] == u2), None)
                if r1 is not None and r2 is not None:
                    key = (min(u1, u2), max(u1, u2))
                    pair_common[key].append((r1, r2))
    for (u1, u2), pairs in pair_common.items():
        if len(pairs) < 3:
            continue
        mae = sum(abs(a - b) for a, b in pairs) / len(pairs)
        pct = max(0, round((1 - mae / 9) * 100))
        compatibility.append({'pair': [u1, u2], 'pct': pct, 'common_films': len(pairs)})
    compatibility.sort(key=lambda x: -x['pct'])

    # Genres: по жанрам, сколько каждый участник посмотрел
    genre_by_user = defaultdict(lambda: defaultdict(int))
    for r in wm_rows + st_rows:
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        g = r.get('genres') if isinstance(r, dict) else (r[7] if len(r) > 7 else None)
        if g:
            for gen in (g or '').split(','):
                gen = gen.strip()
                if gen:
                    genre_by_user[gen][uid] += 1
    genres = []
    for genre, by_mem in sorted(genre_by_user.items(), key=lambda x: -sum(x[1].values())):
        genres.append({
            'genre': genre,
            'by_member': [{'user_id': uid, 'count': c} for uid, c in sorted(by_mem.items(), key=lambda x: -x[1])]
        })
    genres = genres[:8]

    # Heatmap: день -> {user_id: count}
    heatmap = defaultdict(lambda: defaultdict(int))
    for r in wm_rows:
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        dt = r.get('watched_at') if isinstance(r, dict) else r[6]
        if dt:
            d = getattr(dt, 'day', None) or (int(str(dt)[8:10]) if len(str(dt)) >= 10 else None)
            if d:
                heatmap[str(d)][str(uid)] = heatmap[str(d)].get(str(uid), 0) + 1
    for r in st_rows:
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        dt = r.get('watched_date') if isinstance(r, dict) else r[6]
        if dt:
            d = getattr(dt, 'day', None) or (int(str(dt)[8:10]) if len(str(dt)) >= 10 else None)
            if d:
                heatmap[str(d)][str(uid)] = heatmap[str(d)].get(str(uid), 0) + 1
    for r in ratings_in_month:
        uid = r.get('user_id') if isinstance(r, dict) else r[1]
        rt_at = r.get('rated_at') if isinstance(r, dict) else (r[3] if len(r) > 3 else None)
        if rt_at:
            dt = _ensure_tz(rt_at)
            if dt:
                d = dt.day
                heatmap[str(d)][str(uid)] = heatmap[str(d)].get(str(uid), 0) + 1
    heatmap = dict(heatmap)

    # public_slug из group_stats_settings
    public_slug = None
    with db_lock:
        cur.execute("SELECT public_slug FROM group_stats_settings WHERE chat_id = %s AND public_enabled = TRUE", (chat_id,))
        row = cur.fetchone()
        if row:
            public_slug = row.get('public_slug') if isinstance(row, dict) else row[0]

    return {
        'success': True,
        'group': {
            'chat_id': chat_id,
            'title': group_title,
            'members_active': len(members),
            'total_films_alltime': total_films,
            'public_slug': public_slug
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


def get_group_stats_settings(chat_id):
    """Настройки публичности групповой статистики."""
    cur = get_db_cursor()
    with db_lock:
        cur.execute(
            "SELECT public_enabled, public_slug, visible_blocks FROM group_stats_settings WHERE chat_id = %s",
            (chat_id,)
        )
        row = cur.fetchone()
    if not row:
        return {'public_enabled': False, 'public_slug': None, 'visible_blocks': {}}
    vb = row.get('visible_blocks') if isinstance(row, dict) else row[2]
    if isinstance(vb, str):
        import json
        vb = json.loads(vb) if vb else {}
    return {
        'public_enabled': bool(row.get('public_enabled') if isinstance(row, dict) else row[0]),
        'public_slug': row.get('public_slug') if isinstance(row, dict) else row[1],
        'visible_blocks': vb or {}
    }


def set_group_stats_settings(chat_id, public_enabled=None, visible_blocks=None):
    """Обновить настройки публичности группы. Создаёт slug при первом включении."""
    cur = get_db_cursor()
    conn = get_db_connection()
    import json
    with db_lock:
        cur.execute("SELECT public_enabled, public_slug FROM group_stats_settings WHERE chat_id = %s", (chat_id,))
        row = cur.fetchone()
        slug = None
        if not row:
            if public_enabled:
                cur.execute("SELECT name FROM site_sessions WHERE chat_id = %s ORDER BY created_at DESC LIMIT 1", (chat_id,))
                srow = cur.fetchone()
                name = (srow.get('name') if isinstance(srow, dict) else srow[0]) if srow else None
                base = (name or 'group').lower().replace(' ', '_')[:24] if name else 'group'
                slug = f"{base}_{abs(chat_id) % 10000}"
                cur.execute("SELECT 1 FROM group_stats_settings WHERE public_slug = %s", (slug,))
                if cur.fetchone():
                    slug = f"g{abs(chat_id)}"
            cur.execute(
                """INSERT INTO group_stats_settings (chat_id, public_enabled, public_slug, updated_at)
                   VALUES (%s, %s, %s, NOW())
                   ON CONFLICT (chat_id) DO UPDATE SET
                     public_enabled = COALESCE(EXCLUDED.public_enabled, group_stats_settings.public_enabled),
                     public_slug = CASE WHEN EXCLUDED.public_enabled THEN COALESCE(group_stats_settings.public_slug, EXCLUDED.public_slug) ELSE group_stats_settings.public_slug END,
                     updated_at = NOW()""",
                (chat_id, bool(public_enabled) if public_enabled is not None else False, slug)
            )
        else:
            curr_slug = row.get('public_slug') if isinstance(row, dict) else row[1]
            curr_enabled = row.get('public_enabled') if isinstance(row, dict) else row[0]
            slug = curr_slug
            if public_enabled and not curr_slug:
                cur.execute("SELECT name FROM site_sessions WHERE chat_id = %s ORDER BY created_at DESC LIMIT 1", (chat_id,))
                srow = cur.fetchone()
                name = (srow.get('name') if isinstance(srow, dict) else srow[0]) if srow else None
                base = (name or 'group').lower().replace(' ', '_')[:24] if name else 'group'
                slug = f"{base}_{abs(chat_id) % 10000}"
                cur.execute("SELECT 1 FROM group_stats_settings WHERE public_slug = %s AND chat_id != %s", (slug, chat_id))
                if cur.fetchone():
                    slug = f"g{abs(chat_id)}"
            cur.execute(
                """UPDATE group_stats_settings SET
                     public_enabled = COALESCE(%s, public_enabled),
                     public_slug = CASE WHEN %s THEN COALESCE(public_slug, %s) ELSE public_slug END,
                     updated_at = NOW()
                   WHERE chat_id = %s""",
                (public_enabled, bool(public_enabled), slug, chat_id)
            )
        conn.commit()
    return get_group_stats_settings(chat_id)


def increment_stats_share_view(slug, stats_type, month, year):
    """Увеличивает счётчик просмотров ссылки на публичную статистику."""
    cur = get_db_cursor()
    with db_lock:
        cur.execute("""
            INSERT INTO stats_share_views (slug, stats_type, month, year, view_count, updated_at)
            VALUES (%s, %s, %s, %s, 1, NOW())
            ON CONFLICT (slug, stats_type, month, year)
            DO UPDATE SET view_count = stats_share_views.view_count + 1, updated_at = NOW()
        """, (slug, stats_type, month, year))
        conn = get_db_connection()
        conn.commit()


def get_stats_share_view_count(slug, stats_type, month, year):
    """Возвращает количество переходов по ссылке за месяц."""
    cur = get_db_cursor()
    with db_lock:
        cur.execute(
            "SELECT view_count FROM stats_share_views WHERE slug = %s AND stats_type = %s AND month = %s AND year = %s",
            (slug, stats_type, month, year)
        )
        row = cur.fetchone()
    if not row:
        return 0
    return row.get('view_count', 0) if isinstance(row, dict) else (row[0] or 0)


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
    # Публичный просмотр: возвращаем все ачивки (earned + locked) для панели «Все ачивки»
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
    try:
        increment_stats_share_view(slug, 'user', month, year)
    except Exception:
        pass
    return data, None


def get_stats_debug(chat_id, month, year, is_personal=True):
    """Сырые данные для отладки: количество записей в watched_movies, series_tracking, ratings за месяц."""
    cur = get_db_cursor()
    start_ts, end_ts = _month_range(month, year)
    user_id = chat_id if is_personal else None
    out = {'chat_id': chat_id, 'month': month, 'year': year, 'is_personal': is_personal, 'period': f'{month}-{year}'}
    with db_lock:
        if is_personal:
            cur.execute("""
                SELECT COUNT(*), COUNT(DISTINCT film_id) FROM watched_movies
                WHERE chat_id = %s AND user_id = %s AND watched_at >= %s AND watched_at < %s
            """, (chat_id, user_id, start_ts, end_ts))
            r = cur.fetchone()
            out['watched_movies_count'] = r[0] if r else 0
            out['watched_movies_films'] = r[1] if r and len(r) > 1 else 0
            cur.execute("""
                SELECT COUNT(*), COUNT(DISTINCT film_id) FROM series_tracking
                WHERE chat_id = %s AND user_id = %s AND watched = TRUE AND watched_date >= %s AND watched_date < %s
            """, (chat_id, user_id, start_ts, end_ts))
            r = cur.fetchone()
            out['series_tracking_count'] = r[0] if r else 0
            out['series_tracking_series'] = r[1] if r and len(r) > 1 else 0
            cur.execute("""
                SELECT COUNT(*) FROM ratings r
                WHERE r.chat_id = %s AND r.user_id = %s AND r.rated_at >= %s AND r.rated_at < %s
            """, (chat_id, user_id, start_ts, end_ts))
            r = cur.fetchone()
            out['ratings_count'] = r[0] if r else 0
        else:
            cur.execute("""
                SELECT COUNT(*), COUNT(DISTINCT film_id) FROM watched_movies
                WHERE chat_id = %s AND watched_at >= %s AND watched_at < %s
            """, (chat_id, start_ts, end_ts))
            r = cur.fetchone()
            out['watched_movies_count'] = r[0] if r else 0
            cur.execute("""
                SELECT COUNT(*) FROM series_tracking
                WHERE chat_id = %s AND watched = TRUE AND watched_date >= %s AND watched_date < %s
            """, (chat_id, start_ts, end_ts))
            r = cur.fetchone()
            out['series_tracking_count'] = r[0] if r else 0
            cur.execute("""
                SELECT COUNT(*) FROM ratings r
                WHERE r.chat_id = %s AND r.rated_at >= %s AND r.rated_at < %s
            """, (chat_id, start_ts, end_ts))
            r = cur.fetchone()
            out['ratings_count'] = r[0] if r else 0
    return out


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
    try:
        increment_stats_share_view(slug, 'group', month, year)
    except Exception:
        pass
    return data, None
