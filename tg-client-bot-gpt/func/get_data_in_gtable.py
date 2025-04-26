import asyncio
import pandas as pd
import openai
import httpx
from pyrogram.errors import FloodWait

from config import SPREADSHEET_ID, GID, PROXY_SETTINGS, OPENAI_API_KEY

async def get_tg_id(app, username: str) -> int:
    """Получаем tg_id через Pyrogram с обработкой FloodWait"""
    try:
        user = await app.get_users(username)
        return user.id
    except FloodWait as e:
        print(f"Ожидаем {e.value} секунд из-за FloodWait")
        await asyncio.sleep(e.value)
        return await get_tg_id(username)
    except Exception as e:
        print(f"Ошибка получения ID для @{username}: {str(e)}")
        return None

PRODUCT_DESCRIPTION = """🧩 Описание Sboard для промта
Sboard — это российская онлайн-доска для совместной визуальной работы команд: альтернатива Miro с возможностью импорта досок из Миро, оплатой в рублях и on-premise установкой. Подходит для продуктовых команд, agile-коучей, scrum-мастеров, IT-директоров и менеджеров по обучению.

Sboard позволяет:

проводить митинги, ретро и фасилитации;

визуализировать бизнес-процессы, CJM, роадмапы;

использовать шаблоны, таймеры, Agile-игры;

быстро импортировать доски из Miro (со всеми объектами и даже изображениями в оригинальном качестве);

работать в облаке или на своей инфраструктуре (on-premise).

Лицензии бывают годовые и бессрочные"""

def get_openai_client():
    # Создаем транспорт только если указан прокси
    transport = None
    if PROXY_SETTINGS["https"]:
        transport = httpx.HTTPTransport(
            proxy=PROXY_SETTINGS["https"],
            retries=3
        )

    proxy_client = httpx.Client(
        transport=transport,
        timeout=30
    ) if transport else httpx.Client(timeout=30)


    return openai.OpenAI(
        api_key=OPENAI_API_KEY,
        http_client=proxy_client
    )

def generate_greeting(user_message):
    try:
        client = get_openai_client()
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": f"""You are Anya from Sboard.online — a service for collaborative work and team communication.  
Write a short Telegram message (1–2 sentences, in Russian) for a person who is interested in: "{user_message}".  
Include an introduction like “I’m Anya from Sboard.online”, but vary the wording and placement each time (start, middle, or end).  
Keep the tone human, informal and friendly — like a person starting a light conversation.  
Mention {PRODUCT_DESCRIPTION} briefly and naturally.  
End the message with a soft, non-pushy question to invite a reply.  
Do not use emojis or formal sales phrases."""},
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


# def generate_greeting(mes):
#         return 'new 2'

async def process_row(app, row):
    try:
        # Пропускаем уже отправленные сообщения
        if pd.notna(row['send_message']) and row['send_message']:
            print(f'Пропускаем пользователя {row["username"]} (сообщение уже отправлено)')
            return row

        # Проверяем наличие tg_id
        if pd.isna(row['tg_id']):
            username = row['username']
            if pd.notna(username) and username.strip() != '':
                tg_id = await get_tg_id(app, username)
                if tg_id is None:
                    print(f'Пользователь @{username} не найден, удаляем строку')
                    return None  # Помечаем строку для удаления
                row['tg_id'] = tg_id
            else:
                print(f'Не указан username для строки {row.name}, удаляем')
                return None

        if pd.notna(row['descriptions']):
            # generated_message = generate_greeting(row['descriptions'])  # Замените на реальную генерацию
            generated_message = 'new mess'  # Замените на реальную генерацию
            if generated_message:
                row['send_message'] = generated_message
        return row
    except Exception as e:
        print(f"Ошибка для пользователя {row['username']}: {str(e)}")
        return None  # Удаляем строку при ошибке


async def process_dataframe_async(app, df):
    tasks = [process_row(app, row.copy()) for _, row in df.iterrows()]
    results = await asyncio.gather(*tasks)
    return pd.DataFrame([r for r in results if r is not None])

async def get_data_in_gtable(app):
    url = f'https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/gviz/tq?tqx=out:csv&gid={GID}'
    loop = asyncio.get_event_loop()
    df = await loop.run_in_executor(None, pd.read_csv, url)

    rename_dict = {
        'Имя': 'first_name',
        'Фамилия': 'last_name',
        'Должность': 'role',
        'компания': 'company_name',
        'tg': 'username',
        'описание': 'descriptions',
        'факт отправки 1 сообщения': 'send_message',
        'факт получения ответа': 'answer_message'
    }
    df = df.rename(columns=rename_dict)
    required_columns = [
        'tg_id',
        'username',
        'first_name',
        'last_name',
        'phone_number',
        'email',
        'role',
        'company_name',
        'descriptions',
        'send_message',
        'answer_message'
    ]
    df = df.reindex(columns=required_columns)
    processed_df = await process_dataframe_async(app, df)
    processed_df['send_message'] = processed_df['send_message'].fillna(False)
    print("\nРезультаты обработки:")
    print(f"Всего записей до обработки: {len(df)}")
    print(f"Успешно обработано: {len(processed_df)}")
    print(f"Удалено записей: {len(df) - len(processed_df)}")

    return processed_df
