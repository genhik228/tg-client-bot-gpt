import asyncio
import traceback
from datetime import datetime

from pyrogram.errors import FloodWait


async def send_to_user(message, target, message_text, pool, app):
    try:
        async with pool.acquire() as conn:
            start_time = datetime.now()
            print(f"\n{'-' * 50}")
            print(f"🚀 ЗАПУСК РАССЫЛКИ [{start_time.strftime('%H:%M:%S')}]")
            print(f"▪ Целевая аудитория: {'Новые пользователи' if target == '1' else 'Все пользователи'}")
            print(f"▪ Текст сообщения: {message_text[:50]}...")
            print(f"{'-' * 50}\n")

            if target == '1':
                users = await conn.fetch("SELECT * FROM clients")
                print(f"🔍 Найдено {len(users)} пользователей в базе")
            else:
                query = "SELECT * FROM clients WHERE message_to_client IS NULL"
                users = await conn.fetch(query)
                print(f"🔍 Найдено {len(users)} новых пользователей")

            if not users:
                print("⛔ Нет пользователей для рассылки")
                await message.reply("❌ Нет пользователей для рассылки")
                return

            total = len(users)
            success = 0
            updated_users = 0
            errors = []

            progress_msg = await message.reply(f"🚀 Начало рассылки для {total} пользователей...")
            print(f"\n🔹 Начало обработки {total} пользователей")
            for idx, user in enumerate(users, 1):
                current_time = datetime.now().strftime("%H:%M:%S")
                user_info = f"ID: {user['user_id']}" + (f" (@{user['username']})" if user['username'] else "")

                try:
                    print(f"\n[{current_time}] ➡️ Обработка пользователя {user_info}")

                    # Получаем актуальные данные
                    tg_user = await app.get_users(user['user_id'])
                    current_username = tg_user.username

                    # Обновление username
                    if current_username != user['username']:
                        print(f"[{current_time}] 🔄 Обновление username для {user['user_id']}: "
                              f"'{user['username']}' → '{current_username}'")
                        await conn.execute(
                            "UPDATE clients SET username = $1 WHERE user_id = $2",
                            current_username, user['user_id']
                        )
                        updated_users += 1

                    # Отправка сообщения
                    print(f"[{current_time}] 📨 Отправка сообщения для {user_info}")
                    await app.send_message(user['user_id'], message_text)
                    success += 1
                    print(f"[{current_time}] ✅ Успешно отправлено")
                    await asyncio.sleep(3)

                    # Обновление статуса
                    await conn.execute(
                        "UPDATE clients SET message_to_client = $1 WHERE user_id = $2",
                        message_text, user['user_id']
                    )
                    print(f"[{current_time}] 📝 Обновлен статус рассылки")

                except FloodWait as e:
                    print(f"\n[{current_time}] ⚠️ FloodWait: Ожидание {e.value} сек")
                    await asyncio.sleep(e.value + 5)

                    if current_username:
                        print(f"[{current_time}] 🔄 Повторная попытка через username @{current_username}")
                        try:
                            await app.send_message(current_username, message_text)
                            success += 1
                            print(f"[{current_time}] ✅ Успешная повторная отправка")
                        except Exception as e:
                            error_msg = f"[{current_time}] ⛔ Ошибка повтора: {str(e)}"
                            errors.append(error_msg)
                            print(error_msg)

                except Exception as e:
                    error_msg = f"[{current_time}] ⛔ Ошибка: {str(e)}"
                    if "Username not found" in str(e):
                        print(f"[{current_time}] 🗑 Username удален для {user['user_id']}")
                        await conn.execute(
                            "UPDATE clients SET username = NULL WHERE user_id = $1",
                            user['user_id']
                        )
                    errors.append(error_msg)
                    print(error_msg)

                # Прогресс каждые 10 пользователей
                if idx % 10 == 0:
                    progress = (
                        f"\n{'-' * 30}\n"
                        f"📊 Прогресс: {idx}/{total}\n"
                        f"✅ Успешно: {success}\n"
                        f"🔄 Обновлено: {updated_users}\n"
                        f"❌ Ошибок: {len(errors)}\n"
                        f"{'-' * 30}"
                    )
                    await progress_msg.edit_text(progress.split('\n')[1])
                    print(progress)

            # Финальный отчет
            end_time = datetime.now()
            duration = end_time - start_time
            final_report = (
                f"\n{'=' * 50}\n"
                f"🏁 РАССЫЛКА ЗАВЕРШЕНА [{end_time.strftime('%H:%M:%S')}]\n"
                f"⏱ Длительность: {duration.total_seconds():.2f} сек\n"
                f"👥 Всего получателей: {total}\n"
                f"✅ Успешных отправок: {success}\n"
                f"🔄 Обновлено записей: {updated_users}\n"
                f"⛔ Ошибок: {len(errors)}\n"
                f"{'=' * 50}"
            )
            print(final_report)

            if errors:
                print("\nПоследние 5 ошибок:")
                for error in errors[-5:]:
                    print(f"• {error}")

            await message.reply(
                f"📊 Итоговый отчет:\n"
                f"• Обработано: {total}\n"
                f"• Успешно: {success}\n"
                f"• Обновлено: {updated_users}\n"
                f"• Ошибок: {len(errors)}\n"
                f"• Время: {duration.total_seconds():.2f} сек"
            )

    except Exception as e:
        error_msg = f"\n{'!' * 50}\n⛔ КРИТИЧЕСКАЯ ОШИБКА: {str(e)}\n{'!' * 50}"
        print(error_msg)
        traceback.print_exc()
        await message.reply(error_msg)