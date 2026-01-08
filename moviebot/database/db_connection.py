"""
Подключение к базе данных и инициализация таблиц
"""
import psycopg2
import psycopg2.extras
from psycopg2.extras import RealDictCursor
import threading
import logging
from moviebot.config import DATABASE_URL, DEFAULT_WATCHED_EMOJIS

logger = logging.getLogger(__name__)

_thread_local = threading.local()

# Глобальные переменные для подключения
db_lock = threading.Lock()

# Семафор: разрешаем только 2 одновременные операции с БД
# (на Railway CPU слабый, больше 2 — начинаются тормоза)
db_semaphore = threading.Semaphore(2)

logger.info("[DB] Семафор для БД инициализирован (макс. 2 одновременных операции)")

def create_new_connection():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logger.info("Подключение к PostgreSQL успешно!")
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к PostgreSQL: {e}", exc_info=True)
        raise

def get_db_connection():
    if not hasattr(_thread_local, "conn") or _thread_local.conn is None or _thread_local.conn.closed:
        _thread_local.conn = create_new_connection()
    return _thread_local.conn

def get_db_cursor():
    return get_db_connection().cursor(cursor_factory=RealDictCursor)

def init_database():
    """Инициализация базы данных: создание таблиц и миграции"""
    conn = get_db_connection()
    cursor = get_db_cursor()
    
    # Создание таблиц
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS movies (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            link TEXT,
            kp_id TEXT,
            title TEXT,
            year INTEGER,
            genres TEXT,
            description TEXT,
            director TEXT,
            actors TEXT,
            watched INTEGER DEFAULT 0,
            rating REAL DEFAULT NULL,
            is_series INTEGER DEFAULT 0,
            UNIQUE(chat_id, kp_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS settings (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            key TEXT,
            value TEXT,
            UNIQUE(chat_id, key)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS plans (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            film_id INTEGER,
            plan_type TEXT,
            plan_datetime TIMESTAMP WITH TIME ZONE,
            user_id BIGINT,
            ticket_file_id TEXT,
            notification_sent BOOLEAN DEFAULT FALSE,
            ticket_notification_sent BOOLEAN DEFAULT FALSE
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stats (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            username TEXT,
            command_or_action TEXT,
            timestamp TEXT,
            chat_id BIGINT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS series_tracking (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            film_id INTEGER,
            kp_id TEXT,
            user_id BIGINT,
            season_number INTEGER,
            episode_number INTEGER,
            watched BOOLEAN DEFAULT FALSE,
            watched_date TIMESTAMP WITH TIME ZONE,
            UNIQUE(chat_id, film_id, user_id, season_number, episode_number)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS series_subscriptions (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            film_id INTEGER,
            kp_id TEXT,
            user_id BIGINT,
            subscribed BOOLEAN DEFAULT TRUE,
            UNIQUE(chat_id, film_id, user_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ratings (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            film_id INTEGER,
            user_id BIGINT,
            rating INTEGER CHECK(rating BETWEEN 1 AND 10),
            is_imported BOOLEAN DEFAULT FALSE,
            kp_id TEXT,
            UNIQUE(chat_id, film_id, user_id)
        )
    ''')
    
    # Миграция: добавление поля kp_id в ratings для импортированных оценок
    try:
        cursor.execute('ALTER TABLE ratings ADD COLUMN IF NOT EXISTS kp_id TEXT')
        conn.commit()
        logger.info("Миграция: поле kp_id добавлено в ratings")
    except Exception as e:
        logger.debug(f"Поле kp_id уже существует или ошибка: {e}")
        conn.rollback()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS watched_movies (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            film_id INTEGER,
            user_id BIGINT,
            watched_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE(chat_id, film_id, user_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS cinema_votes (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT,
            film_id INTEGER,
            deadline TEXT,
            message_id BIGINT,
            yes_users TEXT DEFAULT '[]',
            no_users TEXT DEFAULT '[]'
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tickets (
            id SERIAL PRIMARY KEY,
            plan_id INTEGER REFERENCES plans(id) ON DELETE CASCADE,
            chat_id BIGINT,
            file_id TEXT,
            file_path TEXT,
            session_datetime TIMESTAMP WITH TIME ZONE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS premiere_reminders (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            kp_id TEXT NOT NULL,
            film_title TEXT,
            premiere_date DATE,
            reminder_sent BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE(chat_id, user_id, kp_id)
        )
    ''')
    
    # Таблицы для подписок
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS subscriptions (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            user_id BIGINT NOT NULL,
            subscription_type TEXT NOT NULL CHECK(subscription_type IN ('personal', 'group')),
            plan_type TEXT NOT NULL CHECK(plan_type IN ('notifications', 'recommendations', 'tickets', 'all')),
            period_type TEXT NOT NULL CHECK(period_type IN ('month', '3months', 'year', 'lifetime')),
            price DECIMAL(10, 2) NOT NULL,
            activated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            next_payment_date TIMESTAMP WITH TIME ZONE,
            expires_at TIMESTAMP WITH TIME ZONE,
            is_active BOOLEAN DEFAULT TRUE,
            cancelled_at TIMESTAMP WITH TIME ZONE,
            telegram_username TEXT,
            group_username TEXT,
            group_size INTEGER DEFAULT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS subscription_features (
            id SERIAL PRIMARY KEY,
            subscription_id INTEGER REFERENCES subscriptions(id) ON DELETE CASCADE,
            feature_type TEXT NOT NULL CHECK(feature_type IN ('notifications', 'recommendations', 'tickets')),
            UNIQUE(subscription_id, feature_type)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS subscription_members (
            id SERIAL PRIMARY KEY,
            subscription_id INTEGER REFERENCES subscriptions(id) ON DELETE CASCADE,
            user_id BIGINT NOT NULL,
            username TEXT,
            added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE(subscription_id, user_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS payments (
            id SERIAL PRIMARY KEY,
            payment_id TEXT UNIQUE NOT NULL,
            yookassa_payment_id TEXT,
            user_id BIGINT NOT NULL,
            chat_id BIGINT NOT NULL,
            subscription_type TEXT NOT NULL CHECK(subscription_type IN ('personal', 'group')),
            plan_type TEXT NOT NULL CHECK(plan_type IN ('notifications', 'recommendations', 'tickets', 'all')),
            period_type TEXT NOT NULL CHECK(period_type IN ('month', '3months', 'year', 'lifetime')),
            group_size INTEGER,
            amount DECIMAL(10, 2) NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            subscription_id INTEGER REFERENCES subscriptions(id) ON DELETE SET NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
    ''')
    
    # Таблица для логирования запросов к API Кинопоиска
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS kinopoisk_api_logs (
            id SERIAL PRIMARY KEY,
            endpoint TEXT NOT NULL,
            method TEXT NOT NULL,
            status_code INTEGER,
            timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            user_id BIGINT,
            chat_id BIGINT,
            kp_id TEXT
        )
    ''')
    
    # Дефолтные настройки
    cursor.execute('INSERT INTO settings (chat_id, key, value) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING', 
                   (-1, "watched_emoji", DEFAULT_WATCHED_EMOJIS))
    
    # Миграции
    try:
        cursor.execute('ALTER TABLE movies ALTER COLUMN chat_id TYPE BIGINT')
        logger.info("Миграция: movies.chat_id изменён на BIGINT")
    except Exception as e:
        logger.debug(f"Миграция movies.chat_id: {e}")
    
    try:
        cursor.execute('ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS group_size INTEGER')
        logger.info("Миграция: subscriptions.group_size добавлен")
    except Exception as e:
        logger.debug(f"Миграция subscriptions.group_size: {e}")
    
    # Миграция: создание таблицы payments (если не существует)
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payments (
                id SERIAL PRIMARY KEY,
                payment_id TEXT UNIQUE NOT NULL,
                yookassa_payment_id TEXT,
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                subscription_type TEXT NOT NULL CHECK(subscription_type IN ('personal', 'group')),
                plan_type TEXT NOT NULL CHECK(plan_type IN ('notifications', 'recommendations', 'tickets', 'all')),
                period_type TEXT NOT NULL CHECK(period_type IN ('month', '3months', 'year', 'lifetime')),
                group_size INTEGER,
                amount DECIMAL(10, 2) NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                subscription_id INTEGER REFERENCES subscriptions(id) ON DELETE SET NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        ''')
        conn.commit()
        logger.info("Миграция: таблица payments создана")
    except Exception as e:
        logger.error(f"Миграция payments: {e}", exc_info=True)
        try:
            conn.rollback()
        except:
            pass
    
    # Миграция: добавление payment_method_id для рекуррентных платежей
    try:
        cursor.execute('ALTER TABLE payments ADD COLUMN IF NOT EXISTS payment_method_id TEXT')
        logger.info("Миграция: payments.payment_method_id добавлен")
    except Exception as e:
        logger.debug(f"Миграция payments.payment_method_id: {e}")
    
    try:
        cursor.execute('ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS payment_method_id TEXT')
        logger.info("Миграция: subscriptions.payment_method_id добавлен")
    except Exception as e:
        logger.debug(f"Миграция subscriptions.payment_method_id: {e}")
    
    # Миграция: добавление telegram_payment_charge_id для возврата звезд
    try:
        cursor.execute('ALTER TABLE payments ADD COLUMN IF NOT EXISTS telegram_payment_charge_id TEXT')
        logger.info("Миграция: payments.telegram_payment_charge_id добавлен")
    except Exception as e:
        logger.debug(f"Миграция payments.telegram_payment_charge_id: {e}")
    
    try:
        cursor.execute('ALTER TABLE settings ALTER COLUMN chat_id TYPE BIGINT')
        logger.info("Миграция: settings.chat_id изменён на BIGINT")
    except Exception as e:
        logger.debug(f"Миграция settings.chat_id: {e}")
    
    try:
        cursor.execute('ALTER TABLE plans ALTER COLUMN chat_id TYPE BIGINT')
        cursor.execute('ALTER TABLE plans ALTER COLUMN user_id TYPE BIGINT')
        logger.info("Миграция: plans.chat_id и plans.user_id изменены на BIGINT")
    except Exception as e:
        logger.debug(f"Миграция plans: {e}")
    
    try:
        cursor.execute('ALTER TABLE stats ALTER COLUMN chat_id TYPE BIGINT')
        cursor.execute('ALTER TABLE stats ALTER COLUMN user_id TYPE BIGINT')
        logger.info("Миграция: stats.chat_id и stats.user_id изменены на BIGINT")
    except Exception as e:
        logger.debug(f"Миграция stats: {e}")
    
    try:
        cursor.execute('ALTER TABLE ratings ALTER COLUMN chat_id TYPE BIGINT')
        cursor.execute('ALTER TABLE ratings ALTER COLUMN user_id TYPE BIGINT')
        logger.info("Миграция: ratings.chat_id и ratings.user_id изменены на BIGINT")
    except Exception as e:
        logger.debug(f"Миграция ratings: {e}")
    
    try:
        cursor.execute('ALTER TABLE ratings ADD COLUMN IF NOT EXISTS is_imported BOOLEAN DEFAULT FALSE')
        logger.info("Миграция: поле is_imported добавлено в ratings")
    except Exception as e:
        logger.debug(f"Миграция ratings.is_imported: {e}")
    
    try:
        cursor.execute('ALTER TABLE cinema_votes ALTER COLUMN chat_id TYPE BIGINT')
        cursor.execute('ALTER TABLE cinema_votes ALTER COLUMN message_id TYPE BIGINT')
        logger.info("Миграция: cinema_votes.chat_id и cinema_votes.message_id изменены на BIGINT")
    except Exception as e:
        logger.debug(f"Миграция cinema_votes: {e}")
    
    try:
        cursor.execute("ALTER TABLE plans ALTER COLUMN plan_datetime TYPE TIMESTAMP WITH TIME ZONE USING plan_datetime::TIMESTAMP WITH TIME ZONE")
        logger.info("Миграция: plan_datetime в plans изменён на TIMESTAMP WITH TIME ZONE")
        conn.commit()
    except Exception as e:
        logger.debug(f"Миграция plan_datetime: {e}")
        try:
            conn.rollback()
        except:
            pass
    
    try:
        cursor.execute("ALTER TABLE plans ADD COLUMN IF NOT EXISTS ticket_file_id TEXT")
        conn.commit()
        logger.info("Поле ticket_file_id добавлено в таблицу plans")
    except Exception as e:
        logger.warning(f"Ошибка при добавлении поля ticket_file_id: {e}")
        conn.rollback()
    
    try:
        cursor.execute("ALTER TABLE plans ADD COLUMN IF NOT EXISTS notification_sent BOOLEAN DEFAULT FALSE")
        conn.commit()
        logger.info("Поле notification_sent добавлено в таблицу plans")
    except Exception as e:
        logger.warning(f"Ошибка при добавлении поля notification_sent: {e}")
        conn.rollback()
    
    # Миграция: добавление полей для онлайн-кинотеатров
    try:
        cursor.execute("ALTER TABLE plans ADD COLUMN IF NOT EXISTS streaming_service TEXT")
        cursor.execute("ALTER TABLE plans ADD COLUMN IF NOT EXISTS streaming_url TEXT")
        cursor.execute("ALTER TABLE plans ADD COLUMN IF NOT EXISTS streaming_done BOOLEAN DEFAULT FALSE")
        conn.commit()
        logger.info("Поля streaming_service, streaming_url и streaming_done добавлены в таблицу plans")
    except Exception as e:
        logger.warning(f"Ошибка при добавлении полей streaming_*: {e}")
        conn.rollback()
    
    try:
        cursor.execute('ALTER TABLE movies ADD COLUMN IF NOT EXISTS is_series INTEGER DEFAULT 0')
        conn.commit()
        logger.info("Поле is_series добавлено в таблицу movies")
    except Exception as e:
        logger.debug(f"Поле is_series уже существует или ошибка: {e}")
        conn.rollback()
    
    # Миграция: изменение типа timestamp в stats с TEXT на TIMESTAMP WITH TIME ZONE
    try:
        # Сначала проверяем текущий тип поля
        cursor.execute("""
            SELECT data_type 
            FROM information_schema.columns 
            WHERE table_name = 'stats' AND column_name = 'timestamp'
        """)
        result = cursor.fetchone()
        if result and result.get('data_type') == 'text':
            # Если поле TEXT, конвертируем его в TIMESTAMP WITH TIME ZONE
            cursor.execute("""
                ALTER TABLE stats 
                ALTER COLUMN timestamp TYPE TIMESTAMP WITH TIME ZONE 
                USING timestamp::TIMESTAMP WITH TIME ZONE
            """)
            conn.commit()
            logger.info("Миграция: timestamp в stats изменён на TIMESTAMP WITH TIME ZONE")
        else:
            logger.debug(f"Поле timestamp уже имеет тип {result.get('data_type') if result else 'неизвестен'}")
    except Exception as e:
        logger.warning(f"Ошибка при миграции timestamp в stats: {e}")
        try:
            conn.rollback()
        except:
            pass
    
    # Удаление дубликатов
    try:
        cursor.execute("""
            DELETE FROM movies a USING (
                SELECT MIN(id) as keep_id, chat_id, kp_id
                FROM movies 
                GROUP BY chat_id, kp_id 
                HAVING COUNT(*) > 1
            ) b
            WHERE a.chat_id = b.chat_id AND a.kp_id = b.kp_id AND a.id != b.keep_id
        """)
        deleted_count = cursor.rowcount
        if deleted_count > 0:
            logger.info(f"Удалено дубликатов фильмов: {deleted_count}")
        conn.commit()
    except Exception as e:
        logger.warning(f"Ошибка при удалении дубликатов: {e}")
        conn.rollback()
    
    # Создание индексов
    try:
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_chat_id ON movies (chat_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_link ON movies (link)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ratings_chat_id ON ratings (chat_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_ratings_film_id ON ratings (film_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_plans_chat_id ON plans (chat_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_plans_film_id ON plans (film_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_plans_datetime ON plans (plan_datetime)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_settings_chat_id ON settings (chat_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_stats_chat_id ON stats (chat_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cinema_votes_chat_id ON cinema_votes (chat_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_cinema_votes_film_id ON cinema_votes (film_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_subscriptions_chat_id ON subscriptions (chat_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_subscriptions_user_id ON subscriptions (user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_subscriptions_active ON subscriptions (is_active, expires_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_subscription_features_subscription_id ON subscription_features (subscription_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_kinopoisk_api_logs_timestamp ON kinopoisk_api_logs (timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_kinopoisk_api_logs_user_id ON kinopoisk_api_logs (user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_kinopoisk_api_logs_chat_id ON kinopoisk_api_logs (chat_id)')
        logger.info("Индексы созданы")
    except Exception as e:
        logger.error(f"Ошибка при создании индексов: {e}", exc_info=True)
        conn.rollback()
    
    # Таблица для промокодов
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS promocodes (
                id SERIAL PRIMARY KEY,
                code TEXT UNIQUE NOT NULL,
                discount_type TEXT NOT NULL CHECK(discount_type IN ('percent', 'fixed')),
                discount_value DECIMAL(10, 2) NOT NULL,
                total_uses INTEGER NOT NULL DEFAULT 0,
                used_count INTEGER NOT NULL DEFAULT 0,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                deactivated_at TIMESTAMP WITH TIME ZONE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS promocode_uses (
                id SERIAL PRIMARY KEY,
                promocode_id INTEGER REFERENCES promocodes(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                payment_id INTEGER REFERENCES payments(id) ON DELETE SET NULL,
                used_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_promocodes_code ON promocodes (code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_promocodes_active ON promocodes (is_active)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_promocode_uses_promocode_id ON promocode_uses (promocode_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_promocode_uses_user_id ON promocode_uses (user_id)')
        logger.info("Таблицы промокодов созданы")
    except Exception as e:
        logger.error(f"Ошибка при создании таблиц промокодов: {e}", exc_info=True)
        conn.rollback()
    
    # Таблица для администраторов
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS event_notifications (
            id SERIAL PRIMARY KEY,
            chat_id BIGINT NOT NULL,
            event_type TEXT NOT NULL,  -- 'random_event', 'weekend_reminder', 'premiere_reminder'
            sent_date DATE NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE(chat_id, event_type, sent_date)
        );
        
        CREATE TABLE IF NOT EXISTS admins (
                id SERIAL PRIMARY KEY,
                user_id BIGINT UNIQUE NOT NULL,
                added_by BIGINT NOT NULL,
                added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                is_active BOOLEAN DEFAULT TRUE
            )
        ''')
        
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_admins_user_id ON admins (user_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_admins_active ON admins (is_active)')
        
        # Добавляем владельца бота (301810276) как администратора, если его еще нет
        cursor.execute('SELECT id FROM admins WHERE user_id = %s', (301810276,))
        if not cursor.fetchone():
            cursor.execute('''
                INSERT INTO admins (user_id, added_by, is_active)
                VALUES (%s, %s, TRUE)
            ''', (301810276, 301810276))
            logger.info("Владелец бота добавлен в таблицу администраторов")
        
        logger.info("Таблица администраторов создана")
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы администраторов: {e}", exc_info=True)
        conn.rollback()
    

    # Миграция: добавление полей для сериалов в таблицу movies
    try:
        cursor.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS poster_url TEXT")
        cursor.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS is_ongoing BOOLEAN DEFAULT FALSE")
        cursor.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS seasons_count INTEGER")
        cursor.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS next_episode TEXT")
        cursor.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS last_api_update TIMESTAMP WITH TIME ZONE")
        cursor.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS added_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()")  # если нужно для сортировки
        conn.commit()
        logger.info("Миграция: поля для сериалов добавлены в таблицу movies")
    except Exception as e:
        logger.warning(f"Ошибка при добавлении полей для сериалов: {e}")
        conn.rollback()

    conn.commit()
    logger.info("База данных инициализирована")