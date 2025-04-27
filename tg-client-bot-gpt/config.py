import os
from dotenv import load_dotenv
load_dotenv()


# Конфигурация
ADMIN_ID = int(os.getenv("ADMIN_ID"))  # Ваш Telegram ID
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
LOGIN = os.getenv("LOGIN")
PHONE_NUMBER = os.getenv("PHONE_NUMBER")
TIME_START = os.getenv("TIME_START")
TIME_END = os.getenv("TIME_END")
# Глобальные переменные

POSTGRES_CONFIG = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "database": os.getenv("POSTGRES_DB"),
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT")
}

DATABASE_URL = f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@postgres:5432/{POSTGRES_CONFIG['database']}"
# DATABASE_URL = f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@localhost:5432/{POSTGRES_CONFIG['database']}"

PROXY_SETTINGS = {
    "http": os.getenv("HTTP_PROXY"),
    "https": os.getenv("HTTPS_PROXY")
}

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
GID = os.getenv("GID")