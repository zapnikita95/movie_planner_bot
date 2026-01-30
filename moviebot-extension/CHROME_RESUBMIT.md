# Повторная отправка в Chrome Web Store (после отказа Yellow Argon)

## Что сделано

- Из описаний убраны все упоминания брендов (Afisha, Kinopoisk, KinoTeatr, Ivi, Okko, Premier, Wink, Start, Amediateka, Rezka, Lordfilm, Allserial, Boxserial и т.д.).
- Обновлены: `EXTENSION_DESCRIPTION.txt`, `PRODUCT_DESCRIPTION.md`, `GOOGLE_STORE_INFO.md`, `PRIVACY_POLICY.md`.
- Добавлен файл **`CHROME_STORE_DESCRIPTION.txt`** — это текст для поля «Подробное описание» в Chrome Web Store. В нём нет названий сервисов и сайтов.

## Как отправить снова

1. **Собрать архив расширения** (в папке `moviebot-extension`):
   ```bash
   ./create-package.sh
   ```
   В родительской папке появится файл `moviebot-extension-v1.1.8.zip`.

2. **В панели разработчика Chrome Web Store:**
   - Загрузи новый пакет `moviebot-extension-v1.1.8.zip`.
   - В поле **«Подробное описание»** вставь текст из файла **`CHROME_STORE_DESCRIPTION.txt`** (целиком). Не добавляй списки сайтов и названия сервисов.
   - Краткое описание в manifest уже нейтральное; при необходимости можно оставить как есть.

3. Отправь на проверку.
