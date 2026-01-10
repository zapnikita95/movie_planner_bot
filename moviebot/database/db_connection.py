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

def migrate_movies_chat_id_to_bigint(cursor, conn):
    try:
        cursor.execute('ALTER TABLE movies ALTER COLUMN chat_id TYPE BIGINT USING chat_id::BIGINT')
        logger.info("Миграция: movies.chat_id изменён на BIGINT")
    except Exception as e:
        logger.debug(f"Миграция movies.chat_id: {e}")
        conn.rollback()

def migrate_subscriptions_group_size(cursor, conn):
    try:
        cursor.execute('ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS group_size INTEGER')
        logger.info("Миграция: subscriptions.group_size добавлен")
    except Exception as e:
        logger.debug(f"Миграция subscriptions.group_size: {e}")
        conn.rollback()

def migrate_payments_table(cursor, conn):
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
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                telegram_payment_charge_id TEXT
            )
        ''')
        logger.info("Миграция: таблица payments создана")
    except Exception as e:
        logger.error(f"Миграция payments: {e}", exc_info=True)
        conn.rollback()

def migrate_payments_payment_method_id(cursor, conn):
    try:
        cursor.execute('ALTER TABLE payments ADD COLUMN IF NOT EXISTS payment_method_id TEXT')
        logger.info("Миграция: payments.payment_method_id добавлен")
    except Exception as e:
        logger.debug(f"Миграция payments.payment_method_id: {e}")
        conn.rollback()

def migrate_subscriptions_payment_method_id(cursor, conn):
    try:
        cursor.execute('ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS payment_method_id TEXT')
        logger.info("Миграция: subscriptions.payment_method_id добавлен")
    except Exception as e:
        logger.debug(f"Миграция subscriptions.payment_method_id: {e}")
        conn.rollback()

def migrate_payments_telegram_payment_charge_id(cursor, conn):
    try:
        cursor.execute('ALTER TABLE payments ADD COLUMN IF NOT EXISTS telegram_payment_charge_id TEXT')
        logger.info("Миграция: payments.telegram_payment_charge_id добавлен")
    except Exception as e:
        logger.debug(f"Миграция payments.telegram_payment_charge_id: {e}")
        conn.rollback()

def migrate_settings_chat_id_to_bigint(cursor, conn):
    try:
        cursor.execute('ALTER TABLE settings ALTER COLUMN chat_id TYPE BIGINT')
        logger.info("Миграция: settings.chat_id изменён на BIGINT")
    except Exception as e:
        logger.debug(f"Миграция settings.chat_id: {e}")
        conn.rollback()

def migrate_plans_chat_id_user_id_to_bigint(cursor, conn):
    try:
        cursor.execute('ALTER TABLE plans ALTER COLUMN chat_id TYPE BIGINT')
        cursor.execute('ALTER TABLE plans ALTER COLUMN user_id TYPE BIGINT')
        logger.info("Миграция: plans.chat_id и plans.user_id изменены на BIGINT")
    except Exception as e:
        logger.debug(f"Миграция plans: {e}")
        conn.rollback()

def migrate_stats_chat_id_user_id_to_bigint(cursor, conn):
    try:
        cursor.execute('ALTER TABLE stats ALTER COLUMN chat_id TYPE BIGINT')
        cursor.execute('ALTER TABLE stats ALTER COLUMN user_id TYPE BIGINT')
        logger.info("Миграция: stats.chat_id и stats.user_id изменены на BIGINT")
    except Exception as e:
        logger.debug(f"Миграция stats: {e}")
        conn.rollback()

def migrate_ratings_chat_id_user_id_to_bigint(cursor, conn):
    try:
        cursor.execute('ALTER TABLE ratings ALTER COLUMN chat_id TYPE BIGINT')
        cursor.execute('ALTER TABLE ratings ALTER COLUMN user_id TYPE BIGINT')
        logger.info("Миграция: ratings.chat_id и ratings.user_id изменены на BIGINT")
    except Exception as e:
        logger.debug(f"Миграция ratings: {e}")
        conn.rollback()

def migrate_ratings_is_imported(cursor, conn):
    try:
        cursor.execute('ALTER TABLE ratings ADD COLUMN IF NOT EXISTS is_imported BOOLEAN DEFAULT FALSE')
        logger.info("Миграция: поле is_imported добавлено в ratings")
    except Exception as e:
        logger.debug(f"Миграция ratings.is_imported: {e}")
        conn.rollback()

def migrate_cinema_votes_chat_id_message_id_to_bigint(cursor, conn):
    try:
        cursor.execute('ALTER TABLE cinema_votes ALTER COLUMN chat_id TYPE BIGINT')
        cursor.execute('ALTER TABLE cinema_votes ALTER COLUMN message_id TYPE BIGINT')
        logger.info("Миграция: cinema_votes.chat_id и cinema_votes.message_id изменены на BIGINT")
    except Exception as e:
        logger.debug(f"Миграция cinema_votes: {e}")
        conn.rollback()

def migrate_plans_plan_datetime_to_timestamptz(cursor, conn):
    try:
        cursor.execute("ALTER TABLE plans ALTER COLUMN plan_datetime TYPE TIMESTAMP WITH TIME ZONE USING plan_datetime::TIMESTAMP WITH TIME ZONE")
        logger.info("Миграция: plan_datetime в plans изменён на TIMESTAMP WITH TIME ZONE")
    except Exception as e:
        logger.debug(f"Миграция plan_datetime: {e}")
        conn.rollback()

def migrate_plans_ticket_file_id(cursor, conn):
    try:
        cursor.execute("ALTER TABLE plans ADD COLUMN IF NOT EXISTS ticket_file_id TEXT")
        logger.info("Поле ticket_file_id добавлено в таблицу plans")
    except Exception as e:
        logger.warning(f"Ошибка при добавлении поля ticket_file_id: {e}")
        conn.rollback()

def migrate_plans_notification_sent(cursor, conn):
    try:
        cursor.execute("ALTER TABLE plans ADD COLUMN IF NOT EXISTS notification_sent BOOLEAN DEFAULT FALSE")
        logger.info("Поле notification_sent добавлено в таблицу plans")
    except Exception as e:
        logger.warning(f"Ошибка при добавлении поля notification_sent: {e}")
        conn.rollback()

def migrate_plans_streaming_fields(cursor, conn):
    try:
        cursor.execute("ALTER TABLE plans ADD COLUMN IF NOT EXISTS streaming_service TEXT")
        cursor.execute("ALTER TABLE plans ADD COLUMN IF NOT EXISTS streaming_url TEXT")
        cursor.execute("ALTER TABLE plans ADD COLUMN IF NOT EXISTS streaming_done BOOLEAN DEFAULT FALSE")
        logger.info("Поля streaming_service, streaming_url и streaming_done добавлены в таблицу plans")
    except Exception as e:
        logger.warning(f"Ошибка при добавлении полей streaming_*: {e}")
        conn.rollback()

def migrate_movies_is_series(cursor, conn):
    try:
        cursor.execute('ALTER TABLE movies ADD COLUMN IF NOT EXISTS is_series INTEGER DEFAULT 0')
        logger.info("Миграция: поле is_series добавлено в таблицу movies")
    except Exception as e:
        logger.warning(f"Ошибка при добавлении поля is_series: {e}")
        conn.rollback()

def migrate_stats_timestamp_to_timestamptz(cursor, conn):
    try:
        cursor.execute("""
            ALTER TABLE stats 
            ALTER COLUMN timestamp TYPE TIMESTAMP WITH TIME ZONE 
            USING timestamp::TIMESTAMP WITH TIME ZONE
        """)
        logger.info("Миграция: timestamp в stats изменён на TIMESTAMP WITH TIME ZONE")
    except Exception as e:
        logger.warning(f"Ошибка при миграции timestamp в stats: {e}")
        conn.rollback()

def migrate_promocodes_tables(cursor, conn):
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
        logger.info("Миграция: таблицы промокодов созданы")
    except Exception as e:
        logger.error(f"Миграция промокодов: {e}", exc_info=True)
        conn.rollback()

def migrate_admins_table(cursor, conn):
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                id SERIAL PRIMARY KEY,
                user_id BIGINT UNIQUE NOT NULL,
                added_by BIGINT NOT NULL,
                added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                is_active BOOLEAN DEFAULT TRUE
            )
        ''')
        logger.info("Миграция: таблица администраторов создана")
    except Exception as e:
        logger.error(f"Миграция admins: {e}", exc_info=True)
        conn.rollback()

def migrate_movies_series_fields(cursor, conn):
    try:
        cursor.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS poster_url TEXT")
        cursor.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS is_ongoing BOOLEAN DEFAULT FALSE")
        cursor.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS seasons_count INTEGER")
        cursor.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS next_episode TEXT")
        cursor.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS last_api_update TIMESTAMP WITH TIME ZONE")
        cursor.execute("ALTER TABLE movies ADD COLUMN IF NOT EXISTS added_date TIMESTAMP WITH TIME ZONE DEFAULT NOW()")
        logger.info("Миграция: поля для сериалов добавлены в таблицу movies")
    except Exception as e:
        logger.warning(f"Ошибка при добавлении полей для сериалов: {e}")
        conn.rollback()

def init_database():
    logger.info("[DB INIT] Начинаем инициализацию базы...")

    # Создаём ВРЕМЕННОЕ соединение ТОЛЬКО для миграций
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        conn.autocommit = False  # будем вручную управлять транзакциями
        cursor = conn.cursor()

        # === Создание таблиц (все IF NOT EXISTS) ===
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
                poster_url TEXT,
                is_ongoing BOOLEAN DEFAULT FALSE,
                seasons_count INTEGER,
                next_episode TEXT,
                last_api_update TIMESTAMP WITH TIME ZONE,
                added_date TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
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
                ticket_notification_sent BOOLEAN DEFAULT FALSE,
                streaming_service TEXT,
                streaming_url TEXT,
                streaming_done BOOLEAN DEFAULT FALSE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stats (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                username TEXT,
                command_or_action TEXT,
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
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
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                payment_method_id TEXT
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
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                telegram_payment_charge_id TEXT
            )
        ''')
        
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
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                id SERIAL PRIMARY KEY,
                user_id BIGINT UNIQUE NOT NULL,
                added_by BIGINT NOT NULL,
                added_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                is_active BOOLEAN DEFAULT TRUE
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS event_notifications (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                event_type TEXT NOT NULL,
                sent_date DATE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                UNIQUE(chat_id, event_type, sent_date)
            )
        ''')
        
        conn.commit()
        logger.info("[DB] Все таблицы созданы")

        # ──────────────────────────────────────────────────────────────
        # Безопасные миграции — добавляем поля, которых может не хватать
        # Выполняется при каждом запуске, но не падает, если поле уже есть
        # ──────────────────────────────────────────────────────────────
        safe_migrations = [
            # Самое важное сейчас — added_by в movies
            "ALTER TABLE movies ADD COLUMN IF NOT EXISTS added_by BIGINT",
            
            # Другие поля, которые раньше были/могут понадобиться позже
            "ALTER TABLE movies ADD COLUMN IF NOT EXISTS source TEXT",
            "ALTER TABLE movies ADD COLUMN IF NOT EXISTS rating_kp REAL",
            "ALTER TABLE movies ADD COLUMN IF NOT EXISTS votes_kp INTEGER",
            
            # На всякий случай для других таблиц (можно потом расширять)
            "ALTER TABLE plans ADD COLUMN IF NOT EXISTS ticket_notification_sent BOOLEAN DEFAULT FALSE",
        ]

        for sql in safe_migrations:
            try:
                cursor.execute(sql)
                conn.commit()
                logger.info(f"[DB Migration] Применено: {sql}")
            except Exception as e:
                # Если ошибка не про "уже существует" — логируем
                err_str = str(e).lower()
                if "already exists" not in err_str and "duplicate" not in err_str:
                    logger.warning(f"[DB Migration] Проблема при выполнении: {sql} → {e}")
                conn.rollback()

        # Добавляем владельца бота как админа (если нет)
        cursor.execute('SELECT id FROM admins WHERE user_id = %s', (301810276,))
        if not cursor.fetchone():
            cursor.execute('''
                INSERT INTO admins (user_id, added_by, is_active)
                VALUES (%s, %s, TRUE)
            ''', (301810276, 301810276))
            conn.commit()
            logger.info("Владелец бота добавлен в таблицу администраторов")

        # Дефолтные настройки
        cursor.execute('INSERT INTO settings (chat_id, key, value) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING', 
                       (-1, "watched_emoji", DEFAULT_WATCHED_EMOJIS))
        conn.commit()

        # Миграции уже применены один раз — отключаем, чтобы запуск был быстрым
        #migrations = [
            #migrate_movies_chat_id_to_bigint,
            #migrate_subscriptions_group_size,
            #migrate_payments_table,
            #migrate_payments_payment_method_id,
            #migrate_subscriptions_payment_method_id,
            #migrate_payments_telegram_payment_charge_id,
            #migrate_settings_chat_id_to_bigint,
            #migrate_plans_chat_id_user_id_to_bigint,
            #migrate_stats_chat_id_user_id_to_bigint,
            #migrate_ratings_chat_id_user_id_to_bigint,
            #migrate_ratings_is_imported,
            #migrate_cinema_votes_chat_id_message_id_to_bigint,
            #migrate_plans_plan_datetime_to_timestamptz,
            #migrate_plans_ticket_file_id,
            #migrate_plans_notification_sent,
            #migrate_plans_streaming_fields,
            #migrate_movies_is_series,
            #migrate_stats_timestamp_to_timestamptz,
            #igrate_promocodes_tables,
            #migrate_admins_table,
            #migrate_movies_series_fields
        #]

        #for migration in migrations:
        #    try:
        #        logger.info(f"[DB] Применяем миграцию: {migration.__name__}")
        ##        migration(cursor, conn)
        #        conn.commit()
        #    except Exception as e:
        #        logger.warning(f"[DB] Миграция {migration.__name__} уже применена или ошибка: {e}")
        #        conn.rollback()

        # Индексы
        try:
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_chat_id ON movies (chat_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_kp_id ON movies (kp_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_ratings_chat_id ON ratings (chat_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_ratings_film_id ON ratings (film_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_plans_chat_id ON plans (chat_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_plans_film_id ON plans (film_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_plans_datetime ON plans (plan_datetime)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_settings_chat_id ON settings (chat_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_stats_chat_id ON stats (chat_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_cinema_votes_chat_id ON cinema_votes (chat_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_subscriptions_chat_id ON subscriptions (chat_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_subscription_features_subscription_id ON subscription_features (subscription_id)')
            conn.commit()
            logger.info("[DB] Индексы созданы")
        except Exception as e:
            logger.warning(f"[DB] Ошибка создания индексов: {e}")
            conn.rollback()


        logger.info("[DB] База данных полностью инициализирована")
        
    except Exception as e:
        logger.error(f"[DB] Критическая ошибка инициализации БД: {e}", exc_info=True)
        if conn:
            conn.rollback()
        raise

    finally:
        if cursor:
            cursor.close()
        # conn.close()   ← НЕ ЗАКРЫВАЕМ здесь!