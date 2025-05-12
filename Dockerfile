# Многоэтапная сборка
FROM python:3.10-slim as builder

WORKDIR /app

# Копируем зависимости из папки проекта
COPY tg-client-bot-gpt/requirements.txt .

# Устанавливаем с кешированием
RUN pip install --user --cache-dir /pip-cache -r requirements.txt

# Финальный образ
FROM python:3.10-slim
WORKDIR /app

COPY --from=builder /root/.local /root/.local
COPY --from=builder /pip-cache /pip-cache

# Настраиваем PATH
ENV PATH=/root/.local/bin:$PATH

# Копируем исходный код из папки проекта
COPY ./tg-client-bot-gpt .
ENTRYPOINT ["python", "main.py"]