#!/bin/bash

# Скрипт для создания ZIP пакета расширения для Chrome Web Store

# Получаем версию из manifest.json
VERSION=$(grep -o '"version": "[^"]*"' manifest.json | sed 's/"version": "//;s/"$//')

# Имя файла пакета
PACKAGE_NAME="../moviebot-extension-v${VERSION}.zip"

# Удаляем старый пакет, если есть
rm -f "$PACKAGE_NAME"

# Создаем ZIP архив, исключая ненужные файлы
zip -r "$PACKAGE_NAME" . \
  -x "*.md" \
  -x ".gitignore" \
  -x ".DS_Store" \
  -x "src/*" \
  -x "promo-*.png" \
  -x "create-package.sh"

echo "✅ Пакет создан: $PACKAGE_NAME"
echo "📦 Версия: $VERSION"
ls -lh "$PACKAGE_NAME"
