# Отладка расширения

## Проблема: "Ошибка сети"

### Что было исправлено:

1. ✅ Добавлены CORS заголовки во все API endpoints
2. ✅ Добавлена обработка OPTIONS preflight запросов
3. ✅ Добавлено логирование запросов для отладки

### Как проверить:

1. **Откройте DevTools в расширении:**
   - Откройте popup расширения
   - Правый клик → "Проверить элемент" (или F12)
   - Перейдите на вкладку "Console"

2. **Проверьте запросы:**
   - Введите код в popup
   - Смотрите в Console на ошибки
   - Перейдите на вкладку "Network" и проверьте запрос к `/api/extension/verify`

3. **Проверьте логи на Railway:**
   - После ввода кода должны появиться логи:
     - `[EXTENSION API] GET /api/extension/verify - code=...`
     - `[EXTENSION API] OPTIONS preflight request for /api/extension/verify`

### Возможные проблемы:

1. **CORS ошибка:**
   - В Console будет: `Access to fetch at '...' from origin 'chrome-extension://...' has been blocked by CORS policy`
   - Решение: Проверьте, что заголовки CORS добавлены (уже исправлено)

2. **Неправильный URL:**
   - Проверьте, что `API_BASE_URL` в `popup.js` и `background.js` правильный
   - Должен быть: `https://web-production-3921c.up.railway.app`

3. **Сервер не отвечает:**
   - Проверьте логи Railway
   - Убедитесь, что сервер запущен

4. **Таблица не создана:**
   - Проверьте, что таблица `extension_links` создана в БД
   - Она создается автоматически при инициализации БД

### Тестирование API напрямую:

Можно протестировать API напрямую в браузере:

```javascript
// В консоли браузера на любой странице
fetch('https://web-production-3921c.up.railway.app/api/extension/verify?code=TEST123')
  .then(r => r.json())
  .then(console.log)
  .catch(console.error)
```

Если видите CORS ошибку - значит проблема в заголовках (но мы их уже добавили).

### Проверка в расширении:

1. Откройте popup расширения
2. Откройте DevTools (F12)
3. В Console выполните:
```javascript
fetch('https://web-production-3921c.up.railway.app/api/extension/verify?code=YOUR_CODE')
  .then(r => r.json())
  .then(console.log)
  .catch(console.error)
```

Если это работает, значит проблема в коде popup.js. Если не работает - проблема в CORS или сервере.
