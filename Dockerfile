FROM python:3.13-slim-bookworm

# Устанавливаем git + runtime-зависимости (ffmpeg для pydub, libpq5 для psycopg2)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        libpq5 \
        git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python3", "-m", "moviebot.main"]