# Переменные окружения Movie Planner Bot

Полное описание всех переменных окружения, используемых в проекте.

---

## Основные настройки бота

| Переменная | Описание | Пример |
|------------|----------|--------|
| `BOT_TOKEN` | Токен Telegram бота от @BotFather | `8554485843:AAFdyCf_...` |
| `IS_PRODUCTION` | Режим продакшена (true/false) | `true` |
| `START_DELAY` | Задержка перед стартом бота в секундах | `5` |
| `MAX_RESTART_ATTEMPTS` | Максимальное количество попыток перезапуска при ошибках | `9` |

---

## База данных (PostgreSQL)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `DATABASE_URL` | Внутренний URL подключения к PostgreSQL (для Railway) | `${{Postgres.DATABASE_URL}}` |
| `DATABASE_PUBLIC_URL` | Публичный URL PostgreSQL (для внешнего доступа) | `${{Postgres.DATABASE_PUBLIC_URL}}` |

**Как управлять**: Эти переменные автоматически подставляются Railway при использовании встроенного PostgreSQL.

---

## API Кинопоиска

### Основной API (kinopoiskapiunofficial.tech)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `KP_TOKEN` | Токен для kinopoiskapiunofficial.tech | `8bbf2210-159d-4e22-...` |

**Где получить**: https://kinopoiskapiunofficial.tech/ → Регистрация → Получить токен

### Резервный API (poiskkino.dev)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `POISKKINO_TOKEN` | Токен для poiskkino.dev | `YP3255F-6XNMYAP-...` |

**Где получить**: Telegram бот @poiskkinodev_bot → Получить токен

### Настройки переключения между API

| Переменная | Описание | Значения | По умолчанию |
|------------|----------|----------|--------------|
| `PRIMARY_API` | Какой API использовать как основной | `kinopoisk_unofficial` или `poiskkino` | `kinopoisk_unofficial` |
| `FALLBACK_ENABLED` | Включить автоматический fallback на резервный API | `true` / `false` | `true` |
| `FALLBACK_THRESHOLD` | Количество последовательных ошибок для переключения на fallback | число | `20` |
| `FALLBACK_RESET_TIMEOUT` | Время в секундах до сброса счётчика ошибок | число | `300` (5 минут) |

**Как работает fallback**:
1. Бот использует API из `PRIMARY_API`
2. При ошибках увеличивается счётчик
3. После `FALLBACK_THRESHOLD` ошибок подряд → переключение на резервный API
4. Через `FALLBACK_RESET_TIMEOUT` секунд без ошибок счётчик сбрасывается

**Примеры настройки**:
```bash
# Использовать kinopoiskapiunofficial как основной, poiskkino как fallback
PRIMARY_API=kinopoisk_unofficial
FALLBACK_ENABLED=true
FALLBACK_THRESHOLD=20

# Использовать только poiskkino (без fallback)
PRIMARY_API=poiskkino
FALLBACK_ENABLED=false

# Агрессивный fallback (переключаться после 5 ошибок)
FALLBACK_THRESHOLD=5
```

---

## Webhook настройки

| Переменная | Описание | Пример |
|------------|----------|--------|
| `USE_WEBHOOK` | Использовать webhook вместо polling | `true` / `false` |
| `WEBHOOK_URL` | URL для webhook (без https://) | `web-production-3921c.up.railway.app` |
| `RAILWAY_PUBLIC_DOMAIN` | Публичный домен Railway | `${{Postgres.RAILWAY_PUBLIC_DOMAIN}}` |

**Как управлять**: 
- `USE_WEBHOOK=true` — для продакшена (Railway)
- `USE_WEBHOOK=false` — для локальной разработки (polling)

---

## Платежи (YooKassa)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `YOOKASSA_SHOP_ID` | ID магазина в YooKassa | `1240036` |
| `YOOKASSA_SECRET_KEY` | Секретный ключ YooKassa | `live_exhp_eUBBC9I7...` |

**Где получить**: https://yookassa.ru/ → Личный кабинет → Настройки магазина

---

## Самозанятость (nalog.ru)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `NALOG_INN` | ИНН самозанятого | `526228327547` |
| `NALOG_PASSWORD` | Пароль от личного кабинета nalog.ru | `WhereIsMyMind123!#` |

**Зачем нужно**: Автоматическое формирование чеков для платежей.

---

## OMDB API (дополнительные данные о фильмах)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `OMDB_API_KEY` | API ключ для OMDB | `642c17df` |

**Где получить**: https://www.omdbapi.com/apikey.aspx

---

## Kaggle (для датасетов актёров/режиссёров)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `KAGGLE_USERNAME` | Имя пользователя Kaggle | `nikitazaporozhets` |
| `KAGGLE_KEY` | API ключ Kaggle | `KGAT_2a5f0f005b6ba...` |

**Где получить**: https://www.kaggle.com/ → Account → API → Create New Token

---

## Настройки поиска и эмбеддингов

| Переменная | Описание | Значения | По умолчанию |
|------------|----------|----------|--------------|
| `FUZZINESS_LEVEL` | Уровень нечёткого поиска (0-100) | `95` = строгий, `80` = мягкий | `95` |
| `USE_FAST_EMBEDDINGS` | Использовать быстрые эмбеддинги | `1` / `0` | `1` |
| `EMBEDDINGS_BATCH_SIZE` | Размер батча для эмбеддингов | число | `128` |
| `FORCE_REBUILD_INDEX` | Принудительно пересобрать индекс при старте | `1` / `0` | `0` |

---

## Настройки топов актёров/режиссёров

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `TOP_ACTORS_COUNT` | Количество актёров в топе | `1000` |
| `TOP_DIRECTORS_COUNT` | Количество режиссёров в топе | `200` |
| `DELETE_TOP_ACTORS_FILE` | Удалить файл топов при старте | `0` |

---

## Whisper (распознавание речи)

| Переменная | Описание | Значения |
|------------|----------|----------|
| `WHISPER_MODEL` | Модель Whisper для распознавания | `tiny`, `base`, `small`, `medium`, `large` |

**Рекомендации**:
- `tiny` — быстро, низкое качество
- `small` — баланс скорости и качества (рекомендуется)
- `large` — медленно, высокое качество

---

## Быстрая настройка для Railway

Минимальный набор переменных для запуска:

```bash
# Обязательные
BOT_TOKEN=ваш_токен_бота
DATABASE_URL=${{Postgres.DATABASE_URL}}
KP_TOKEN=ваш_токен_кинопоиска

# Рекомендуемые
IS_PRODUCTION=true
USE_WEBHOOK=true
WEBHOOK_URL=ваш_домен.up.railway.app
POISKKINO_TOKEN=токен_для_fallback
FALLBACK_ENABLED=true
```

---

## Советы по безопасности

1. **Никогда не коммитьте** токены и пароли в git
2. Используйте **переменные Railway** вместо `.env` файлов в продакшене
3. Регулярно **ротируйте токены** (особенно после утечек)
4. Для `YOOKASSA_SECRET_KEY` используйте **live_ ключи только в продакшене**
