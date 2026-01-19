# Структура расширения Movie Planner Bot

## Текущая структура папок

```
moviebot-extension/
├── manifest.json                    # Манифест расширения (Manifest V3)
├── background.js                    # Service Worker
├── popup.html                       # HTML popup
├── popup.css                        # Стили popup
├── popup.js                         # Логика popup
├── content/
│   ├── content-kp.js               # Content script для Кинопоиска
│   ├── content-imdb.js             # Content script для IMDb
│   ├── content-letterboxd.js        # Content script для Letterboxd
│   └── content-tickets.js          # Content script для сайтов с билетами
├── icons/
│   ├── icon16.png                  # Иконка 16x16
│   ├── icon48.png                  # Иконка 48x48
│   └── icon128.png                 # Иконка 128x128
├── README.md                        # Документация
├── SETUP.md                         # Инструкция по настройке
└── .gitignore                       # Git ignore файл
```

## Описание файлов

### Основные файлы

- **manifest.json** - Конфигурация расширения (Manifest V3)
  - Permissions: storage, activeTab, scripting, contextMenus, downloads, notifications
  - Host permissions для всех необходимых сайтов
  - Content scripts для каждого типа страниц

- **background.js** - Service Worker
  - Обработка сообщений от content scripts
  - Управление контекстным меню
  - API_BASE_URL: `https://web-production-3921c.up.railway.app`

- **popup.html/css/js** - Интерфейс расширения
  - Авторизация по коду
  - Отображение информации о фильме
  - Форма планирования просмотра
  - API_BASE_URL: `https://web-production-3921c.up.railway.app`

### Content Scripts

- **content-kp.js** - Кинопоиск
  - Извлекает kp_id из URL
  - Определяет тип (фильм/сериал)
  - Matches: `*://*.kinopoisk.ru/film/*`, `*://*.kinopoisk.ru/series/*`

- **content-imdb.js** - IMDb
  - Извлекает imdb_id из URL
  - Matches: `*://*.imdb.com/title/*`

- **content-letterboxd.js** - Letterboxd
  - Извлекает imdb_id из ссылок на странице
  - Fallback на название и год
  - Matches: `*://letterboxd.com/film/*`

- **content-tickets.js** - Сайты с билетами
  - Добавляет кнопку "Отправить в Movie Planner"
  - Matches: Афиша, Кинопоиск, КиноТеатр и др.

## API Endpoints

Все endpoints используют базовый URL: `https://web-production-3921c.up.railway.app`

- `GET /api/extension/verify?code=XXX` - Проверка кода авторизации
- `GET /api/extension/film-info?kp_id=XXX&chat_id=XXX` - Информация о фильме по kp_id
- `GET /api/extension/film-info?imdb_id=XXX&chat_id=XXX` - Информация о фильме по imdb_id
- `POST /api/extension/add-film` - Добавление фильма в базу
- `POST /api/extension/create-plan` - Создание плана просмотра

## Установка

1. Откройте Chrome/Edge → `chrome://extensions/`
2. Включите "Режим разработчика"
3. Нажмите "Загрузить распакованное расширение"
4. Выберите папку `moviebot-extension`

## Готово к использованию

✅ API_BASE_URL настроен на `web-production-3921c.up.railway.app`
✅ Manifest.json обновлен согласно требованиям
✅ Content scripts переименованы и настроены
✅ Иконки созданы из изображения
✅ Структура соответствует требованиям
