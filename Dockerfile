FROM python:3.13-slim-bookworm

# Устанавливаем git + runtime-зависимости (ffmpeg для pydub, libpq5 для psycopg2)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        libpq5 \
        git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копируем requirements.txt отдельно для лучшего кэширования
COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копируем код проекта (лишние файлы исключены через .dockerignore)
COPY . .

CMD ["python3", "-m", "moviebot.main"]