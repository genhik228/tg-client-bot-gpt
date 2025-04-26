import asyncio
import traceback
import asyncpg
from typing import List, Dict
from pyrogram.errors import FloodWait
import log_print as pr


async def get_users_with_sent_messages(conn: asyncpg.Connection) -> List[Dict]:
    """
    Возвращает список пользователей с отправленными сообщениями (send_flag = True)

    :param conn: подключение к базе данных asyncpg
    :return: список словарей с данными пользователей
    """
    try:
        # Выполняем SQL-запрос
        records = await conn.fetch(
            "SELECT tg_id FROM users WHERE send_flag = TRUE AND answer_message IS NULL"
        )

        # Конвертируем записи в список словарей
        return [dict(record) for record in records]

    except asyncpg.PostgresError as e:
        print(f"Ошибка при работе с базой данных: {str(e)}")
        return []
    except Exception as e:
        print(f"Неизвестная ошибка: {str(e)}")
        return []

async def get_chat_history_safe(app, chat_id, limit=100, offset_id=0):
    for _ in range(3):
        try:
            return [msg async for msg in app.get_chat_history(
                chat_id=chat_id,
                limit=limit,
                offset_id=offset_id
            )]
        except FloodWait as e:
            await asyncio.sleep(e.value + 2)
        except Exception as e:
            pr.print_error(f"Ошибка: {str(e)}")
            break
    return []

async def get_full_chat_history(app, chat_id, last_message_id):
    pr.print_header(f"НАЧАЛО СБОРА ИСТОРИИ ЧАТА {chat_id}")
    messages = []
    offset_id = 0
    chunk_counter = 0
    total_messages = 0
    while True:
        try:
            chunk = await get_chat_history_safe(app, chat_id, 100, offset_id)
            if not chunk:
                # pr.print_warning("Пустая порция сообщений - завершаем сбор")
                break
            filtered = [msg for msg in chunk if msg.id > last_message_id]
            filtered_size = len(filtered)

            if not filtered:
                # pr.print_warning("Нет новых сообщений - завершаем сбор")
                break

            messages.extend(filtered)
            total_messages += filtered_size
            offset_id = chunk[-1].id
            chunk_counter += 1

            if chunk_counter % 5 == 0:
                pr.print_warning(f"Пауза для предотвращения FloodWait...")
                await asyncio.sleep(3)
            else:
                await asyncio.sleep(1)
        except Exception as e:
            pr.print_error(f"Критическая ошибка при получении истории: {str(e)}")
            traceback.pr.print_exc()
            break

    return messages[::-1]

async def update_answer(app, conn):
    users = await get_users_with_sent_messages(conn)
    pr.print_header(f"НАЧАЛО Обвноление сообщений")
    for index, user in enumerate(users, start=1):
        sucs = False
        try:
            message_clients = await get_full_chat_history(app, user['tg_id'], 0)
            for msg in message_clients:
                if hasattr(msg, 'from_user') and msg.from_user.id == user['tg_id']:
                    first_user_message = msg.text or msg.caption or "отправлен медиа файл без сообщения"
                    sucs = True
                    async with conn.transaction():
                        await conn.execute('''
                                                        UPDATE users 
                                                        SET answer_message = $1 
                                                        WHERE tg_id = $2
                                                    ''', first_user_message, user['tg_id'])
                    break
            if sucs:
                pr.print_success('Пользователь ответи')
            else:
                pr.print_error('Пользователь не ответил')
            pr.print_header(f"ЗАВЕРШЕНИЕ СБОРА ДЛЯ ЧАТА {user['tg_id']}")

        except Exception as e:
            error_msg = f"Ошибка у клиента {user['tg_id']}: {str(e)}"
            pr.print_error(error_msg)
