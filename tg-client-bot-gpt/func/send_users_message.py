import asyncio
import random
from datetime import datetime

import pytz
from pyrogram import Client
from pyrogram.errors import (
    UsernameNotOccupied,
    UsernameInvalid,
    PeerIdInvalid,
    FloodWait,
    RPCError
)
import log_print as pr
from config import TIME_START, TIME_END
from db import save_message_users


def is_time_between_moscow(start_str, end_str):
    # Получаем текущее время в часовом поясе Москвы
    tz_moscow = pytz.timezone("Europe/Moscow")
    current_time = datetime.now(tz_moscow).time()

    # Конвертируем строки start_str и end_str во временные объекты
    start_obj = datetime.strptime(start_str, "%H:%M").time()
    end_obj = datetime.strptime(end_str, "%H:%M").time()

    return start_obj <= current_time <= end_obj

async def send_messages(app: Client, conn):
    """Рассылает сообщения пользователям с send_flag=False"""
    try:
        # Получаем пользователей для рассылки
        users = await conn.fetch(
            "SELECT tg_id, username, send_message "
            "FROM users "
            "WHERE send_flag = FALSE "
            "AND send_message IS NOT NULL"
        )

        if not users:
            print("Нет пользователей для рассылки")
            return "Нет пользователей для рассылки"

        print(f"Начало рассылки для {len(users)} пользователей...")
        for user in users:
            print()
            if is_time_between_moscow(TIME_START, TIME_END) == False:
                return f'⛔ Время для рассылки с {TIME_START} до {TIME_END}'
            random_sleep = random.randint(1, 5)
            try:
                # Извлекаем данные
                tg_id = user['tg_id']
                username = user['username']
                message = user['send_message']

                # Пытаемся отправить по username
                if username and username.strip():
                    try:
                        sent_message = await app.send_message(
                            chat_id=username,
                            text=message
                        )
                        print(f"Отправлено через username @{username} {sent_message.id}" )
                    except (PeerIdInvalid, UsernameNotOccupied) as e:
                        print(f"Ошибка: пользователь @{username} не найден. {str(e)}")
                        sent_message = None
                    except FloodWait as e:
                        print(f"Превышен лимит запросов. Ждем {e.value} сек.")
                        await asyncio.sleep(e.value)
                        sent_message = await app.send_message(chat_id=username, text=message)
                    except Exception as e:
                        print(f"Неизвестная ошибка при отправке @{username}: {str(e)}")
                        sent_message = None
                    except (UsernameNotOccupied, UsernameInvalid):
                        print(f"Неверный username @{username}, пробую через tg_id {tg_id}")
                        sent_message = await app.send_message(
                            chat_id=tg_id,
                            text=message
                        )
                        print(f"Отправлено через tg_id {tg_id}")

                    if sent_message is not None:
                        try:
                            await save_message_users(conn, sent_message, direction='spam')
                            print(f"Сообщение сохранено")
                        except Exception as e:
                            print(f"Ошибка сохранения сообщения: {str(e)}")
                    else:
                        print(f"Не удалось отправить сообщение @{username}")
                # Если username отсутствует, отправляем по tg_id
                else:
                    await app.send_message(
                        chat_id=tg_id,
                        text=message
                    )
                    print(f"Отправлено через tg_id {tg_id}")

                # Помечаем как отправленное
                await conn.execute(
                    "UPDATE users "
                    "SET send_flag = TRUE "
                    "WHERE tg_id = $1",
                    tg_id
                )

                # Задержка между сообщениями
                print(f'Время ожидания до следующей отправки составит {random_sleep} секунд')
                await asyncio.sleep(random_sleep)

            except FloodWait as e:
                print(f"FloodWait: Ожидаем {e.value} секунд")
                await asyncio.sleep(e.value + 3)  # Добавляем запас

            except PeerIdInvalid:
                print(f"Ошибка: Некорректный ID {tg_id}")
                await mark_as_failed(conn, tg_id)

            except RPCError as e:
                print(f"Ошибка Telegram API для {tg_id}: {str(e)}")
                await mark_as_failed(conn, tg_id)

            except Exception as e:
                print(f"Неизвестная ошибка для {tg_id}: {str(e)}")
                await mark_as_failed(conn, tg_id)

        print(" ✅ Рассылка завершена")
        return "✅ Рассылка завершена"
    except Exception as e:
        print(f"Критическая ошибка рассылки: {str(e)}")
        return f"Критическая ошибка рассылки: {str(e)}"
        raise

async def mark_as_failed(conn, tg_id):
    """Помечает отправку как неудачную"""
    await conn.execute(
        "UPDATE users "
        "SET send_flag = TRUE, " 
        "answer_message = 'Ошибка отправки' "
        "WHERE tg_id = $1",
        tg_id
    )