# Отладка CORS ошибок в расширении

## Проблема
Расширение получает ошибки CORS при запросах к API:
- `No 'Access-Control-Allow-Origin' header is present`
- `The 'Access-Control-Allow-Origin' header contains multiple values '*, *'`

## Решение

### 1. Проверка заголовков в ответе сервера

Откройте DevTools в Chrome (F12) → вкладка Network:
1. Откройте расширение и попробуйте выполнить действие (например, ввести код)
2. Найдите запрос к `web-production-3921c.up.railway.app/api/extension/...`
3. Кликните на запрос → вкладка **Headers**
4. Проверьте раздел **Response Headers**:
   - Должен быть `Access-Control-Allow-Origin: *`
   - Должен быть только ОДИН такой заголовок (не два!)

### 2. Проверка логов на Railway

1. Откройте Railway Dashboard → ваш проект
2. Перейдите в раздел **Deployments** → выберите последний деплой
3. Откройте **Logs**
4. Ищите строки с `[EXTENSION API]`:
   ```
   [EXTENSION API] GET /api/extension/verify - code=...
   [EXTENSION API] POST /api/extension/add-film - kp_id=...
   [EXTENSION API] POST /api/extension/film-info - kp_id=...
   ```

### 3. Проверка через curl

Проверьте, что сервер возвращает правильные заголовки:

```bash
# Проверка OPTIONS запроса (preflight)
curl -X OPTIONS \
  -H "Origin: chrome-extension://mglliogakfilkboahpaaehphpdflkodn" \
  -H "Access-Control-Request-Method: GET" \
  -v \
  https://web-production-3921c.up.railway.app/api/extension/verify?code=TEST

# Проверка GET запроса
curl -X GET \
  -H "Origin: chrome-extension://mglliogakfilkboahphpdflkodn" \
  -v \
  https://web-production-3921c.up.railway.app/api/extension/verify?code=TEST
```

В выводе должны быть строки:
```
< Access-Control-Allow-Origin: *
< Access-Control-Allow-Headers: Content-Type,Authorization
< Access-Control-Allow-Methods: GET,POST,OPTIONS
```

### 4. Проверка в расширении

1. Откройте расширение в Chrome: `chrome://extensions/`
2. Найдите ваше расширение → нажмите **"Просмотреть представление service worker"** (для background.js)
3. Откройте DevTools для popup: кликните правой кнопкой на иконку расширения → **"Проверить всплывающее окно"**
4. В консоли выполните:
   ```javascript
   fetch('https://web-production-3921c.up.railway.app/api/extension/verify?code=TEST')
     .then(r => {
       console.log('Status:', r.status);
       console.log('Headers:', [...r.headers.entries()]);
       return r.json();
     })
     .then(data => console.log('Data:', data))
     .catch(err => console.error('Error:', err));
   ```

### 5. Частые проблемы

#### Проблема: Дублирование заголовков
**Симптом:** `The 'Access-Control-Allow-Origin' header contains multiple values '*, *'`

**Решение:** 
- Убедитесь, что используется только `after_request` hook
- Не вызывайте `add_cors_headers()` вручную в endpoint'ах
- Используйте `response.headers['...'] = '...'` вместо `response.headers.add('...', '...')`

#### Проблема: Заголовки не добавляются
**Симптом:** `No 'Access-Control-Allow-Origin' header is present`

**Решение:**
- Проверьте, что `after_request` hook зарегистрирован
- Проверьте, что путь начинается с `/api/extension/`
- Проверьте логи Railway - возможно, запрос не доходит до сервера

#### Проблема: OPTIONS запрос не обрабатывается
**Симптом:** Preflight запрос возвращает 405 или ошибку

**Решение:**
- Убедитесь, что endpoint принимает метод `OPTIONS`
- Проверьте, что `after_request` hook обрабатывает OPTIONS запросы

### 6. Проверка кода

Убедитесь, что в `web_app.py`:
1. `after_request` hook зарегистрирован ДО всех роутов
2. Используется `response.headers['...'] = '...'` (не `add()`)
3. Нет вызовов `add_cors_headers()` в endpoint'ах (или они не добавляют заголовки)

### 7. Тестирование после исправления

1. Перезагрузите расширение в Chrome
2. Очистите кэш браузера (Ctrl+Shift+Delete)
3. Попробуйте снова выполнить действие в расширении
4. Проверьте Network tab в DevTools - заголовки должны быть правильными

## Логи для отладки

В логах Railway должны появляться строки:
```
[EXTENSION API] OPTIONS preflight request for /api/extension/verify
[EXTENSION API] GET /api/extension/verify - code=...
[EXTENSION API] POST /api/extension/film-info - kp_id=...
```

Если их нет - запрос не доходит до сервера (проблема с CORS на этапе preflight).
