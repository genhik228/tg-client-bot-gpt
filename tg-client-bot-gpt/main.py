from datetime import datetime
import os
import asyncpg
import asyncio
import logging
from pyrogram import Client, filters, idle
from pyrogram.types import Message
from pyrogram.errors import FloodWait
import traceback
from config import POSTGRES_CONFIG, API_ID, API_HASH, ADMIN_ID, DATABASE_URL
from db import create_tables, save_message_users, save_media
from func.get_answer_for_client import update_answer
from func.get_data_in_gtable import get_data_in_gtable
import log_print as pr
from func.send_users_message import send_messages
import math
from asyncpg.exceptions import DataError

from asyncpg.exceptions import CannotConnectNowError


pool = None
app = None

logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# async def process_missing_ids(df: pd.DataFrame, delay: int = 1) -> pd.DataFrame:
#     missing_ids = df[df['tg_id'].isna()]
#     for idx, row in missing_ids.iterrows():
#         username = row['username']
#         if pd.notna(username) and username.strip() != '':
#             tg_id = await get_tg_id(username)
#             df.at[idx, 'tg_id'] = int(tg_id)
#             await asyncio.sleep(delay)
#     return df.dropna(subset=['tg_id'])


def clean_value(value, max_length=None):
    """Очистка и валидация значений"""
    if isinstance(value, float):
        if math.isnan(value):
            return None
        value = str(int(value)) if value.is_integer() else str(value)

    if value is None:
        return None

    # Обрезаем строки до максимальной длины
    str_value = str(value)
    if max_length and len(str_value) > max_length:
        return str_value[:max_length]
    return str_value


async def save_users(users_dict, conn):
    if not users_dict:
        pr.print_warning("Нет новых пользователей для сохранения")
        return "Нет новых пользователей для сохранения"
    try:
        processed_users = {}
        for k, v in users_dict.items():
            try:
                tg_id = int(float(k)) if isinstance(k, (float, str)) else int(k)
                processed_users[tg_id] = {
                    'tg_id': tg_id,
                    **{key: clean_value(val) for key, val in v.items()}
                }
            except (ValueError, TypeError) as e:
                pr.print_error(f"Ошибка конвертации tg_id {k}: {e}")
                continue

        # Получаем существующие записи
        existing = await conn.fetch(
            "SELECT tg_id, send_message FROM users WHERE tg_id = ANY($1::bigint[])",
            list(processed_users.keys())
        )
        sent_ids = {rec['tg_id'] for rec in existing if rec['send_message']}
        new_users = []
        for tg_id, u in processed_users.items():
            if tg_id in sent_ids:
                pr.print_step(f"Пользователь {tg_id} уже получил сообщение. Пропускаем.")
                continue
            try:
                user_tuple = (
                                    tg_id,
                                    clean_value(u.get('username')),
                                    clean_value(u.get('first_name')),
                                    clean_value(u.get('last_name')),
                                    clean_value(u.get('phone_number')),
                                    clean_value(u.get('email')),
                                    clean_value(u.get('role')),
                                    clean_value(u.get('company_name')),
                                    clean_value(u.get('descriptions')),
                                    clean_value(u.get('send_message')),  # Статус отправки
                                    None  # answer_message
                                    )
                new_users.append(user_tuple)
            except Exception as e:
                pr.print_error(f"Ошибка формирования записи {tg_id}: {e}")
                continue

        if not new_users:
            pr.print_warning("Нет данных для обновления")
            return "Нет данных для обновления"

        # Пакетная вставка с обработкой ошибок
        try:
            await conn.executemany('''
                INSERT INTO users (
                    tg_id, username, first_name, last_name, 
                    phone_number, email, role, company_name, 
                    descriptions, send_message, answer_message
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                ON CONFLICT (tg_id) DO UPDATE SET
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    phone_number = EXCLUDED.phone_number,
                    email = EXCLUDED.email,
                    role = EXCLUDED.role,
                    company_name = EXCLUDED.company_name,
                    descriptions = EXCLUDED.descriptions,
                    send_message = EXCLUDED.send_message
            ''', new_users)
            pr.print_success(f"Успешно обновлено {len(new_users)} записей")
            return f"Успешно обновлено {len(new_users)} записей"
        except DataError as e:
            pr.print_error(f"Ошибка формата данных: {e}")
            # Логируем проблемные данные
            for i, row in enumerate(new_users):
                try:
                    await conn.execute('''
                        INSERT INTO users 
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ''', row)
                except Exception as single_error:
                    pr.print_error(f"Ошибка в строке {i}: {single_error}")

    except Exception as e:
        pr.print_error(f"Критическая ошибка: {e}")
        raise

user_states = {}


async def init_db():
    max_retries = 5
    retry_delay = 5  # seconds

    for attempt in range(max_retries):
        try:
            print('DATABASE_URL', DATABASE_URL)
            pool = await asyncpg.create_pool(DATABASE_URL)
            async with pool.acquire() as conn:
                await create_tables(conn)
            return pool
        except (CannotConnectNowError, ConnectionRefusedError) as e:
            print(f"Попытка {attempt + 1}/{max_retries}: Ошибка подключения - {str(e)}")
            await asyncio.sleep(retry_delay)

    raise RuntimeError("Не удалось подключиться к PostgreSQL после 5 попыток")


async def main():
    global pool, app
    pr.print_header("ЗАПУСК ПАРСЕРА TELEGRAM")
    print(DATABASE_URL)
    print('123')
    menu_text = (
        "**🏁 ПАНЕЛЬ УПРАВЛЕНИЯ**\n\n"
        "1. 🕷️ Создать сообщения пользователям\n"
        "2. 📨 Отправить сообщения\n"
        "3. 📊  Обновление сообщений от клиентов\n"
        "Выберите цифру (1-6):"
    )
    try:
        # pool = await asyncpg.create_pool(**POSTGRES_CONFIG)
        print(DATABASE_URL)
        # pool = await asyncpg.create_pool(DATABASE_URL)
        await init_db()
        async with pool.acquire() as conn:
            await create_tables(conn)

        app = Client(
            name="session",
            api_id=API_ID,
            api_hash=API_HASH,
            workdir="sessions"
        )
        @app.on_message(filters.private)
        async def log_all_messages(client: Client, message: Message):
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f'================= {current_time} =========================\nТекст: {message.text}\nПользователь: {message.from_user.id} {message.from_user.username}')
            try:
                async with pool.acquire() as conn:
                    if message.from_user.id == ADMIN_ID and not message.from_user.is_bot:
                        if message.text == '1':
                            df = await get_data_in_gtable(app)
                            users_dict = {row['tg_id']: row.to_dict() for _, row in df.iterrows()}
                            messages_save_users = await save_users(users_dict, conn)
                            await app.send_message(chat_id=ADMIN_ID, text=messages_save_users)
                        elif message.text == '2':
                            message_s = await send_messages(app, conn)
                            await app.send_message(chat_id=ADMIN_ID, text=message_s)
                        elif message.text == '3':
                            await update_answer(app, conn)
                            await app.send_message(chat_id=ADMIN_ID, text="Обновление завершено")
                        else:
                            print(f'===== Админ  {current_time} ===========\nТекст: {message.text}')
                            await save_message_users(conn, message, direction='out')
                            if message.media:
                                await save_media(client, message, conn, message.caption or "")
                        await app.send_message(chat_id=ADMIN_ID, text=menu_text)

                    else:
                        await save_message_users(conn, message, direction='in')
                        if message.media:
                            await save_media(client, message, conn, message.caption or "")
            except FloodWait as e:
                wait_time = e.value + 5
                pr.print_warning(f"Требуется подождать {wait_time} секунд")
                await asyncio.sleep(wait_time)
                return await log_all_messages(client, message)
            except Exception as e:
                await message.reply(f"⛔ Критическая ошибка: {str(e)}")
                traceback.print_exc()

        await app.start()
        me = await app.get_me()
        pr.print_success(f"Бот @{me.username} [ID:{me.id}] успешно запущен!")
        try:
            await app.send_message(ADMIN_ID, "🤖 Парсер запущен и готов к работе!")
            # Отправка меню
            await app.send_message(chat_id=ADMIN_ID, text=menu_text)

        except Exception as e:
            pr.print_warning(f"Не удалось отправить сообщение администратору: {str(e)}")
            pr.print_warning("Убедитесь, что бот добавлен в контакты и может писать вам.")

        await idle()

    except Exception as e:
        pr.print_error(f"Фатальная ошибка: {str(e)}")
    finally:
        if pool:
            await pool.close()
        if app:
            await app.stop()
        pr.print_success("Все соединения корректно закрыты")
        os._exit(0)  # Полное завершение процесса

if __name__ == "__main__":
    asyncio.run(main())