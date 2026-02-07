"""
Подключение к базе данных и инициализация таблиц
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import threading
import logging
from moviebot.config import DATABASE_URL, DEFAULT_WATCHED_EMOJIS

logger = logging.getLogger(__name__)

# Глобальные переменные для подключения
_conn = None
_cursor = None
# ВАЖНО: Используем RLock (реентерабельный lock) вместо Lock, чтобы избежать дедлоков
# когда одна функция с db_lock вызывает другую функцию с db_lock в том же потоке
db_lock = threading.RLock()

def get_db_connection():
    """Получить подключение к БД"""
    global _conn
    if _conn is None or _conn.closed:
        if not DATABASE_URL:
            raise ValueError("DATABASE_URL не задан!")
        try:
            # Закрываем старое соединение, если оно есть
            if _conn is not None and not _conn.closed:
                try:
                    _conn.close()
                except:
                    pass
            _conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
            logger.info("Подключение к PostgreSQL успешно!")
        except Exception as e:
            logger.error(f"Не удалось подключиться к БД: {e}")
            _conn = None
            raise
    return _conn

def get_db_cursor():
    """Получить курсор БД"""
    global _cursor
    conn = get_db_connection()
    # Проверяем, что курсор существует и не закрыт
    need_new_cursor = False
    if _cursor is None:
        need_new_cursor = True
    else:
        try:
            # Проверяем, закрыт ли курсор
            if _cursor.closed:
                need_new_cursor = True
            else:
                # Проверяем, закрыто ли соединение
                try:
                    if conn.closed:
                        need_new_cursor = True
                except:
                    need_new_cursor = True
        except:
            # Если произошла ошибка при проверке, пересоздаем курсор
            need_new_cursor = True
    
    if need_new_cursor:
        try:
            if _cursor is not None:
                try:
                    _cursor.close()
                except:
                    pass
        except:
            pass
        _cursor = conn.cursor()
    return _cursor

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
    
    # Миграция: добавление поля year в ratings для импортированных оценок
    try:
        cursor.execute('ALTER TABLE ratings ADD COLUMN IF NOT EXISTS year INTEGER')
        conn.commit()
        logger.info("Миграция: поле year добавлено в ratings")
    except Exception as e:
        logger.debug(f"Поле year уже существует или ошибка: {e}")
        conn.rollback()
    
    # Миграция: добавление поля genres в ratings для импортированных оценок
    try:
        cursor.execute('ALTER TABLE ratings ADD COLUMN IF NOT EXISTS genres TEXT')
        conn.commit()
        logger.info("Миграция: поле genres добавлено в ratings")
    except Exception as e:
        logger.debug(f"Поле genres уже существует или ошибка: {e}")
        conn.rollback()
    
    # Миграция: добавление поля type в ratings для импортированных оценок (FILM или TV_SERIES)
    try:
        cursor.execute('ALTER TABLE ratings ADD COLUMN IF NOT EXISTS type TEXT')
        conn.commit()
        logger.info("Миграция: поле type добавлено в ratings")
    except Exception as e:
        logger.debug(f"Поле type уже существует или ошибка: {e}")
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
    
    # Миграция: напоминание об оценке фильма через 3 часа после просмотра (только фильмы, не сериалы)
    try:
        cursor.execute("ALTER TABLE plans ADD COLUMN IF NOT EXISTS rate_reminder_sent BOOLEAN DEFAULT FALSE")
        conn.commit()
        logger.info("Поле rate_reminder_sent добавлено в таблицу plans")
    except Exception as e:
        logger.warning(f"Ошибка при добавлении поля rate_reminder_sent: {e}")
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
    
    # Миграция: добавление custom_title для пользовательских названий планов/мероприятий (без film_id)
    try:
        cursor.execute("ALTER TABLE plans ADD COLUMN IF NOT EXISTS custom_title TEXT")
        conn.commit()
        logger.info("Миграция: добавлено поле custom_title в таблицу plans")
    except Exception as e:
        logger.warning(f"Ошибка при добавлении custom_title в plans: {e}")
        try:
            conn.rollback()
        except:
            pass
    
    try:
        cursor.execute('ALTER TABLE movies ADD COLUMN IF NOT EXISTS is_series INTEGER DEFAULT 0')
        conn.commit()
        logger.info("Поле is_series добавлено в таблицу movies")
    except Exception as e:
        logger.debug(f"Поле is_series уже существует или ошибка: {e}")
        conn.rollback()
    
    try:
        cursor.execute('ALTER TABLE movies ADD COLUMN IF NOT EXISTS online_link TEXT')
        conn.commit()
        logger.info("Поле online_link добавлено в таблицу movies")
    except Exception as e:
        logger.debug(f"Поле online_link уже существует или ошибка: {e}")
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
    
    # Таблица для кодов расширения
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS extension_links (
                code TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
                used BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_extension_links_code ON extension_links (code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_extension_links_expires ON extension_links (expires_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_extension_links_chat_id ON extension_links (chat_id)')
        logger.info("Таблица extension_links создана")
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы extension_links: {e}", exc_info=True)
        conn.rollback()
    
    # Таблица сессий сайта (личный кабинет)
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS site_sessions (
                id SERIAL PRIMARY KEY,
                token TEXT UNIQUE NOT NULL,
                chat_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                name TEXT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP WITH TIME ZONE NOT NULL
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_site_sessions_token ON site_sessions (token)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_site_sessions_chat_id ON site_sessions (chat_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_site_sessions_expires ON site_sessions (expires_at)')
        conn.commit()
        logger.info("Таблица site_sessions создана")
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы site_sessions: {e}", exc_info=True)
        conn.rollback()
    
    # Таблицы для тегов/подборок фильмов
    try:
        # Таблица тегов (подборок)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tags (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                short_code TEXT UNIQUE NOT NULL,
                created_by BIGINT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_short_code ON tags (short_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tags_created_by ON tags (created_by)')
        
        # Таблица связи тегов с фильмами (kp_id)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tag_movies (
                id SERIAL PRIMARY KEY,
                tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
                kp_id TEXT NOT NULL,
                is_series BOOLEAN DEFAULT FALSE,
                added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(tag_id, kp_id)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tag_movies_tag_id ON tag_movies (tag_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tag_movies_kp_id ON tag_movies (kp_id)')
        
        # Таблица связи пользователей с тегами (какие фильмы из тега добавлены в базу пользователя)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_tag_movies (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
                film_id INTEGER NOT NULL,
                added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, chat_id, tag_id, film_id)
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_tag_movies_user_chat ON user_tag_movies (user_id, chat_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_tag_movies_tag_id ON user_tag_movies (tag_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_tag_movies_film_id ON user_tag_movies (film_id)')
        
        # Таблица событий «добавил подборку» (переход по ссылке + «Добавить в базу»)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tag_add_events (
                id SERIAL PRIMARY KEY,
                tag_id INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tag_add_events_tag_id ON tag_add_events (tag_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_tag_add_events_added_at ON tag_add_events (added_at)')
        
        # Флаг «бэкфилл выполнен» для ретроспективных данных
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tag_add_events_backfill_done (
                id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1)
            )
        ''')
        
        # Ретроспективный бэкфилл: по user_tag_movies создаём по одному событию на (user, chat, tag)
        cursor.execute('SELECT 1 FROM tag_add_events_backfill_done LIMIT 1')
        if cursor.fetchone() is None:
            cursor.execute('''
                INSERT INTO tag_add_events (tag_id, user_id, chat_id, added_at)
                SELECT tag_id, user_id, chat_id, MIN(added_at)
                FROM user_tag_movies
                GROUP BY tag_id, user_id, chat_id
            ''')
            cursor.execute('INSERT INTO tag_add_events_backfill_done (id) VALUES (1) ON CONFLICT (id) DO NOTHING')
            logger.info("tag_add_events: ретроспективный бэкфилл выполнен")
        
        logger.info("Таблицы для тегов созданы")
    except Exception as e:
        logger.error(f"Ошибка при создании таблиц для тегов: {e}", exc_info=True)
        conn.rollback()
    
    # Миграция: rated_at для статистики по месяцам
    try:
        cursor.execute('ALTER TABLE ratings ADD COLUMN IF NOT EXISTS rated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()')
        conn.commit()
        logger.info("Миграция: ratings.rated_at добавлен")
    except Exception as e:
        logger.debug(f"Миграция ratings.rated_at: {e}")
        try:
            conn.rollback()
        except Exception:
            pass

    # Миграция: backfill rated_at и watched_at = январь 2026 для старых записей
    try:
        cursor.execute("""
            UPDATE ratings SET rated_at = '2026-01-15 12:00:00+00'
            WHERE rated_at IS NULL OR rated_at = '2025-01-15 12:00:00+00'::timestamptz
        """)
        r_cnt = cursor.rowcount
        cursor.execute("""
            UPDATE watched_movies SET watched_at = '2026-01-15 12:00:00+00'
            WHERE watched_at IS NULL OR watched_at = '2025-01-15 12:00:00+00'::timestamptz
        """)
        w_cnt = cursor.rowcount
        conn.commit()
        if r_cnt or w_cnt:
            logger.info("Миграция: backfill rated_at=%s, watched_at=%s записей в январь 2026", r_cnt, w_cnt)
    except Exception as e:
        logger.debug(f"Миграция backfill: {e}")
        try:
            conn.rollback()
        except Exception:
            pass

    # Таблица настроек публичной групповой статистики
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS group_stats_settings (
                chat_id BIGINT PRIMARY KEY,
                public_enabled BOOLEAN NOT NULL DEFAULT false,
                public_slug VARCHAR(64) UNIQUE,
                visible_blocks JSONB NOT NULL DEFAULT '{"summary":true,"mvp":true,"top_films":true,"rating_breakdown":true,"leaderboard":true,"controversial":true,"compatibility":true,"genres":true,"achievements":true,"heatmap":true}'::jsonb,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            )
        ''')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_group_stats_slug ON group_stats_settings(public_slug) WHERE public_slug IS NOT NULL')
        conn.commit()
        logger.info("Таблица group_stats_settings создана")
    except Exception as e:
        logger.error(f"Таблица group_stats_settings: {e}", exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass

    # Таблица настроек публичной личной статистики
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_stats_settings (
                user_id BIGINT PRIMARY KEY,
                public_enabled BOOLEAN NOT NULL DEFAULT false,
                public_slug VARCHAR(64) UNIQUE,
                visible_blocks JSONB NOT NULL DEFAULT '{"summary":true,"top_films":true,"rating_breakdown":true,"cinema":true,"platforms":true,"watched_list":true}'::jsonb,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
            )
        ''')
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_user_stats_slug ON user_stats_settings(public_slug) WHERE public_slug IS NOT NULL')
        conn.commit()
        logger.info("Таблица user_stats_settings создана")
    except Exception as e:
        logger.error(f"Таблица user_stats_settings: {e}", exc_info=True)
        try:
            conn.rollback()
        except Exception:
            pass

    # Таблица походов в кино (для статистики; планы удаляются, а записи остаются)
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS cinema_screenings (
                chat_id BIGINT NOT NULL,
                user_id BIGINT NOT NULL,
                film_id INTEGER NOT NULL,
                screening_date DATE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (chat_id, user_id, film_id)
            )
        ''')
        conn.commit()
        logger.info("Таблица cinema_screenings создана")
    except Exception as e:
        logger.debug(f"Таблица cinema_screenings: {e}")
        try:
            conn.rollback()
        except Exception:
            pass

    # Таблица цветов аватаров участников группы
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS member_avatar_colors (
                chat_id BIGINT,
                user_id BIGINT,
                color VARCHAR(7) NOT NULL,
                PRIMARY KEY (chat_id, user_id)
            )
        ''')
        conn.commit()
        logger.info("Таблица member_avatar_colors создана")
    except Exception as e:
        logger.debug(f"Таблица member_avatar_colors: {e}")
        try:
            conn.rollback()
        except Exception:
            pass

    conn.commit()
    logger.info("База данных инициализирована")


