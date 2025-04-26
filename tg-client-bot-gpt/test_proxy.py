import openai
import httpx
import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла
load_dotenv()

# Получаем настройки прокси из переменных окружения
PROXY_SETTINGS = {
    "http": os.getenv("HTTP_PROXY"),
    "https": os.getenv("HTTPS_PROXY")
}


def get_openai_client():
    # Создаем транспорт только если указан прокси
    transport = None
    if PROXY_SETTINGS["https"]:
        transport = httpx.HTTPTransport(
            proxy=PROXY_SETTINGS["https"],
            retries=3
        )

    # Инициализируем клиент с транспортом (если есть прокси)
    proxy_client = httpx.Client(
        transport=transport,
        timeout=30
    ) if transport else httpx.Client(timeout=30)

    return openai.OpenAI(
        api_key=os.getenv("OPENAI_API_KEY"),
        http_client=proxy_client
    )
PRODUCT_DESCRIPTION = """🧩 Описание Sboard для промта
Sboard — это российская онлайн-доска для совместной визуальной работы команд: альтернатива Miro с возможностью импорта досок из Миро, оплатой в рублях и on-premise установкой. Подходит для продуктовых команд, agile-коучей, scrum-мастеров, IT-директоров и менеджеров по обучению.

Sboard позволяет:

проводить митинги, ретро и фасилитации;

визуализировать бизнес-процессы, CJM, роадмапы;

использовать шаблоны, таймеры, Agile-игры;

быстро импортировать доски из Miro (со всеми объектами и даже изображениями в оригинальном качестве);

работать в облаке или на своей инфраструктуре (on-premise).

Лицензии бывают годовые и бессрочные"""
def generate_greeting(user_message):
    try:
        client = get_openai_client()
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": f"""
                            Сгенерируй приветственное сообщение для пользователя.
                            Информация о продукте: {PRODUCT_DESCRIPTION}
                            Данные пользователя: {user_message}
                            Требования:
                            - Длина до 200 символов
                            - Персонализация на основе данных пользователя
                            - Упоминание 1-2 ключевых преимуществ
                            - Естественный дружеский тон
                            """},
                {"role": "user", "content": "Напиши приветственное сообщение"}
            ],
            temperature=0.7,
            max_tokens=150
        )
        return response.choices[0].message.content.strip()

    except httpx.ProxyError as e:
        print(f"Ошибка подключения к прокси: {e}")
        return None
    except openai.APIError as e:
        print(f"Ошибка OpenAI API: {e}")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")
        return None


# Пример использования
user_message = "опытом в прототипировании, тестировании, взаимодействии с разработчиками и приоритизации задач"
print(generate_greeting(user_message))
