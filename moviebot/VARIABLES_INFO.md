# Переменные окружения Movie Planner Bot

Документация по всем переменным окружения, используемым в проекте.

---

## Основные настройки бота

| Переменная | Описание | Пример |
|------------|----------|--------|
| `BOT_TOKEN` | **Обязательно.** Токен Telegram бота от @BotFather | `8554485843:AAFdyCf_...` |
| `IS_PRODUCTION` | Режим работы. `true` = продакшен, `false` = разработка | `true` |
| `START_DELAY` | Задержка перед стартом бота (секунды). Полезно для ожидания БД | `5` |
| `MAX_RESTART_ATTEMPTS` | Максимум попыток перезапуска при критических ошибках | `9` |

---

## База данных (PostgreSQL)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `DATABASE_URL` | **Обязательно.** URL подключения к PostgreSQL (внутренний для Railway) | `${{Postgres.DATABASE_URL}}` |
| `DATABASE_PUBLIC_URL` | Публичный URL БД (для внешних подключений) | `${{Postgres.DATABASE_PUBLIC_URL}}` |

---

## API Кинопоиска

### Основной API (kinopoiskapiunofficial.tech)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `KP_TOKEN` | **Обязательно.** API токен для kinopoiskapiunofficial.tech. Получить: https://kinopoiskapiunofficial.tech/ | `8bbf2210-159d-...` |

### Резервный API (poiskkino.dev)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `POISKKINO_TOKEN` | API токен для poiskkino.dev. Получить через бота @poiskkinodev_bot | `YP3255F-6XNM...` |

### Управление переключением API

| Переменная | Описание | Значения по умолчанию |
|------------|----------|----------------------|
| `PRIMARY_API` | Основной API для запросов | `kinopoisk_unofficial` (или `poiskkino`) |
| `FALLBACK_ENABLED` | Включить автоматический fallback на резервный API при ошибках | `true` |
| `FALLBACK_THRESHOLD` | Количество последовательных ошибок для переключения на fallback | `20` |
| `FALLBACK_RESET_TIMEOUT` | Время (сек) до сброса счётчика ошибок и возврата на основной API | `300` |

**Как это работает:**
1. Бот использует API из `PRIMARY_API` как основной
2. Если основной API возвращает ошибки `FALLBACK_THRESHOLD` раз подряд, бот переключается на резервный
3. Через `FALLBACK_RESET_TIMEOUT` секунд без ошибок счётчик сбрасывается
4. Установи `FALLBACK_ENABLED=false` чтобы отключить автопереключение

**Переключение вручную:**
- Чтобы использовать poiskkino.dev как основной: `PRIMARY_API=poiskkino`
- Чтобы использовать kinopoiskapiunofficial.tech: `PRIMARY_API=kinopoisk_unofficial`

---

## Платежи (YooKassa)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `YOOKASSA_SHOP_ID` | ID магазина в YooKassa | `1240036` |
| `YOOKASSA_SECRET_KEY` | Секретный ключ API YooKassa | `live_exhp_eUBBC9I7...` |

---

## Налоги (самозанятый)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `NALOG_INN` | ИНН самозанятого для формирования чеков | `526228327547` |
| `NALOG_PASSWORD` | Пароль от ЛК nalog.ru | `WhereIsMyMind123!#` |

---

## Webhook настройки

| Переменная | Описание | Пример |
|------------|----------|--------|
| `USE_WEBHOOK` | Использовать webhook (`true`) или polling (`false`) | `true` |
| `WEBHOOK_URL` | Домен для webhook (без https://) | `web-production-3921c.up.railway.app` |
| `RAILWAY_PUBLIC_DOMAIN` | Публичный домен Railway | `${{Postgres.RAILWAY_PUBLIC_DOMAIN}}` |

---

## Shazam и распознавание

| Переменная | Описание | Пример |
|------------|----------|--------|
| `WHISPER_MODEL` | Модель Whisper для распознавания речи. Варианты: `tiny`, `base`, `small`, `medium`, `large` | `small` |
| `OMDB_API_KEY` | API ключ для OMDB (дополнительная информация о фильмах) | `642c17df` |

---

## Kaggle (база актёров/режиссёров)

| Переменная | Описание | Пример |
|------------|----------|--------|
| `KAGGLE_USERNAME` | Логин на Kaggle | `nikitazaporozhets` |
| `KAGGLE_KEY` | API ключ Kaggle | `KGAT_2a5f0f005b...` |
| `TOP_ACTORS_COUNT` | Количество топовых актёров для загрузки | `1000` |
| `TOP_DIRECTORS_COUNT` | Количество топовых режиссёров для загрузки | `200` |
| `DELETE_TOP_ACTORS_FILE` | Удалять временные файлы актёров после загрузки. `1` = да | `0` |

---

## Эмбеддинги и поиск

| Переменная | Описание | Пример |
|------------|----------|--------|
| `USE_FAST_EMBEDDINGS` | Использовать быстрые эмбеддинги. `1` = да, `0` = нет | `1` |
| `EMBEDDINGS_BATCH_SIZE` | Размер батча для генерации эмбеддингов | `128` |
| `FORCE_REBUILD_INDEX` | Принудительно пересобрать индекс при старте. `1` = да | `0` |
| `FUZZINESS_LEVEL` | Уровень нечёткости поиска (0-100). Чем выше, тем точнее совпадение | `95` |

---

## Быстрые сценарии настройки

### Переключить на poiskkino.dev как основной API
```
PRIMARY_API=poiskkino
FALLBACK_ENABLED=true
```

### Отключить fallback (только один API)
```
FALLBACK_ENABLED=false
```

### Увеличить порог для fallback (больше терпимости к ошибкам)
```
FALLBACK_THRESHOLD=50
FALLBACK_RESET_TIMEOUT=600
```

### Использовать только poiskkino.dev без fallback
```
PRIMARY_API=poiskkino
FALLBACK_ENABLED=false
```

---

## Мониторинг состояния API

В коде можно проверить текущее состояние:

```python
from moviebot.api.kinopoisk_api import get_api_status

status = get_api_status()
# Вернёт:
# {
#     'primary_api': 'kinopoisk_unofficial',
#     'fallback_enabled': True,
#     'using_fallback': False,
#     'primary_error_count': 0,
#     'fallback_threshold': 20,
#     'current_api': 'kinopoisk_unofficial'
# }
```

---

## Примечания

1. **Обязательные переменные**: `BOT_TOKEN`, `DATABASE_URL`, `KP_TOKEN`
2. **Рекомендуется**: установить `POISKKINO_TOKEN` для fallback
3. **Railway**: переменные с `${{...}}` автоматически подставляются Railway
4. **Безопасность**: никогда не коммитьте токены в репозиторий
