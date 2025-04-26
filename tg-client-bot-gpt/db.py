import os
from datetime import datetime
from mimetypes import guess_extension

from pyrogram import Client
from pyrogram.types import Message

import log_print as pr
from config import ADMIN_ID, LOGIN


async def create_tables(conn):
    tables = [
        ('users', '''
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                tg_id BIGINT UNIQUE,
                send_flag BOOLEAN NOT NULL DEFAULT FALSE,
                username VARCHAR(32),
                first_name VARCHAR(64),
                last_name VARCHAR(64),
                phone_number TEXT,
                email TEXT,
                role TEXT,
                company_name TEXT,
                descriptions TEXT,
                send_message TEXT,
                answer_message TEXT
            )
        '''),
        ('messages', '''
            CREATE TABLE IF NOT EXISTS messages  (
                message_id BIGINT PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    sender_id BIGINT,
    sender_username VARCHAR(255),
    text TEXT,
    recipient_id BIGINT,
    direction VARCHAR(8) NOT NULL CHECK (direction IN ('in', 'out')),
    timestamp TIMESTAMP WITH TIME ZONE,
    media_group_id BIGINT
        )
    '''),
        ('media_attachments', '''
CREATE TABLE IF NOT EXISTS media_attachments (
    media_id SERIAL PRIMARY KEY,
    message_id BIGINT REFERENCES messages(message_id) ON DELETE CASCADE,
    media_type VARCHAR(50) NOT NULL,
    file_path TEXT NOT NULL,
    file_id VARCHAR(255) NOT NULL UNIQUE,
    file_name VARCHAR(255),
    mime_type VARCHAR(100),
    file_size BIGINT,
    duration INT,
    width INT,
    height INT,
    caption TEXT,
    performer VARCHAR(255),
    title VARCHAR(255),
    file_extension VARCHAR(10)
        )
    '''),
        ('users_idx', 'CREATE INDEX IF NOT EXISTS users_idx ON users(send_message)'),
        ('users_tg_idx', 'CREATE INDEX IF NOT EXISTS users_tg_idx ON users(tg_id)'),
        ('idx_media_group', 'CREATE INDEX IF NOT EXISTS idx_media_group ON messages(media_group_id)'),
        ('idx_user_messages', 'CREATE INDEX IF NOT EXISTS idx_user_messages ON messages(sender_id)'),
    ]

    try:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
        pr.print_success("Расширение pg_trgm успешно активировано")
    except Exception as e:
        pr.print_error(f"Ошибка активации расширения pg_trgm: {str(e)}")
        pr.print_warning("Для работы поиска по регулярным выражениям необходимо:")
        pr.print_warning("1. Установить расширение postgresql-contrib")
        pr.print_warning("2. Дать права суперпользователя для создания расширений")
        pr.print_warning("ИЛИ выполнить вручную в БД: CREATE EXTENSION IF NOT EXISTS pg_trgm;")

    for table_name, query in tables:
        try:
            await conn.execute(query)
            pr.print_success(f"Таблица {table_name} создана/проверена")
        except Exception as e:
            pr.print_error(f"Ошибка создания таблицы {table_name}: {str(e)}")


# async def save_message_users(conn, message):
#     await conn.execute(
#         """INSERT INTO messages (
#             message_id, chat_id, user_id, username,
#             text, timestamp, media_group_id
#         ) VALUES ($1, $2, $3, $4, $5, $6, $7)
#         ON CONFLICT (message_id) DO NOTHING""",
#         message.id,
#         message.chat.id,
#         message.from_user.id,
#         message.from_user.username,
#         message.text or message.caption or "",
#         message.date,
#         message.media_group_id
#     )
async def save_message_users(conn, message: Message, direction: str):
    """Универсальное сохранение сообщений с проверкой всех атрибутов"""
    message_id = message.id

    if direction == 'spam':
        direction = "out"
        chat_id = message.chat.id
        sender_id = ADMIN_ID
        sender_username = LOGIN
        recipient_id = chat_id
    else:
        chat_id = message.chat.id if message.chat else None
        sender = message.from_user or message.sender_chat
        sender_id = sender.id
        sender_username = sender.username if hasattr(sender, 'username') else None
        recipient_id = chat_id if direction == "out" else ADMIN_ID

    # Безопасное получение текста
    text = (
        message.text
        or message.caption
    )

    # Безопасное получение media_group_id
    media_group_id = getattr(message, 'media_group_id', None)

    await conn.execute(
        """INSERT INTO messages (
            message_id, chat_id, sender_id, recipient_id,
            sender_username, text, timestamp, 
            media_group_id, direction
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (message_id) DO NOTHING""",
        message_id,
        chat_id,
        sender_id,
        recipient_id,
        sender_username,
        text,
        message.date,
        media_group_id,
        direction
    )

    # except Exception as e:
    #     print(f"Ошибка сохранения сообщения {message.id}:")
    #     print(f"Направление: {direction}")
    #     print(f"Тип отправителя: {type(sender) if 'sender' in locals() else 'UNDEFINED'}")
    #     print(f"Ошибка: {str(e)}")
    #     raise

async def ensure_user_dir(sender_id: int) -> str:
    path = os.path.join("data", str(sender_id))
    os.makedirs(path, exist_ok=True)
    return path


async def download_media_file(client: Client, message: Message, media_type: str) -> dict:
    user_dir = await ensure_user_dir(message.from_user.id)
    file_info = {
        "file_path": None,
        "extension": None,
        "original_name": None
    }

    try:
        media_obj = getattr(message, media_type)
        mime_type = getattr(media_obj, 'mime_type', '')
        original_name = getattr(media_obj, 'file_name', None)
        message_id = message.id  # Получаем ID сообщения

        # Базовое имя файла: {message_id}_
        base_prefix = f"{message_id}_"

        # Определяем расширение
        if original_name:
            # Разделяем имя и расширение
            name_part, extension = os.path.splitext(original_name)
            final_name = f"{base_prefix}{original_name}"
        else:
            # Генерируем имя на основе типа медиа
            extension = guess_extension(mime_type.split(';')[0]) or '.bin'
            type_name = media_type  # document, photo, video и т.д.
            final_name = f"{base_prefix}{type_name}{extension}"

        # Принудительные правила для конкретных типов
        if media_type == 'photo':
            extension = '.jpg'
            final_name = f"{base_prefix}photo{extension}"

        elif media_type == 'document' and not original_name:
            # Для документов без имени используем общее имя
            final_name = f"{base_prefix}document{extension}"

        # Обработка файлов типа ".1" (без имени с расширением)
        if not original_name and extension and final_name.startswith('.'):
            final_name = f"{base_prefix}{media_type}{extension}"

        # Формирование полного пути
        file_path = os.path.join(user_dir, final_name)

        # Скачивание файла
        downloaded_path = await client.download_media(
            message,
            file_name=file_path
        )

        if downloaded_path:
            file_info["file_path"] = os.path.relpath(downloaded_path)
            file_info["extension"] = extension.lstrip('.') if extension else ''
            file_info["original_name"] = original_name  # Сохраняем оригинальное имя

    except Exception as e:
        print(f"Ошибка скачивания: {e}")

    return file_info


async def save_media(client, message, conn, caption: str):
    media_type = None
    media_attrs = [
        'photo', 'video', 'audio', 'voice',
        'document', 'animation', 'sticker'
    ]

    for attr in media_attrs:
        if getattr(message, attr, None):
            media_type = attr
            break

    if not media_type:
        return

    media_obj = getattr(message, media_type)
    file_info = await download_media_file(client, message, media_type)

    if not file_info["file_path"]:
        print(f"Не удалось скачать файл: {message.id}")
        return
    print(file_info)
    await conn.execute(
        """INSERT INTO media_attachments (
            message_id, media_type, file_path, file_id, file_name,
            mime_type, file_size, duration, width, height,
            caption, performer, title, file_extension
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
        )""",
        message.id,
        media_type,
        file_info["file_path"],
        media_obj.file_id,
        file_info.get("original_name") or os.path.basename(file_info["file_path"]),  # <-- исправлено здесь
        getattr(media_obj, 'mime_type', None),
        getattr(media_obj, 'file_size', None),
        getattr(media_obj, 'duration', None),
        getattr(media_obj, 'width', None),
        getattr(media_obj, 'height', None),
        caption,
        getattr(media_obj, 'performer', None),
        getattr(media_obj, 'title', None),
        file_info["extension"]
    )