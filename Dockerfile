# Многоэтапная сборка для кеширования
FROM python:3.10-slim as builder

WORKDIR /app

# Установка зависимостей с кешированием
COPY requirements.txt .
RUN pip install --user --cache-dir /pip-cache -r requirements.txt

# Финальный образ
FROM python:3.10-slim
WORKDIR /app

# Копируем зависимости и кеш
COPY --from=builder /root/.local /root/.local
COPY --from=builder /pip-cache /pip-cache

# Настраиваем PATH
ENV PATH=/root/.local/bin:$PATH

# Копируем исходный код
COPY . .

ENTRYPOINT ["python", "main.py"]