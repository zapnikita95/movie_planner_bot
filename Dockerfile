# Dockerfile
FROM python:3.13-slim-bookworm

# Устанавливаем только runtime-зависимости
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Рабочая директория
WORKDIR /app

# Копируем только requirements и ставим пакеты без кэша
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем остальной код
COPY . .

# Запуск бота (точно как в railpack)
CMD ["python3", "-m", "moviebot.main"]
