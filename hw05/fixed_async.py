"""
Асинхронный сервис обработки данных.
В этом файле скрыто несколько блокирующих операций.
Найди и исправь их все.

Скопируй этот файл в hw05/fixed_async.py и внеси исправления.
Для каждого исправления добавь комментарий, объясняющий:
- Что именно блокирует event loop
- Почему это проблема
- Как исправление решает проблему
"""

import asyncio
import json
import time
from pathlib import Path

import aiofiles
import aiohttp

# ---------------------------------------------------------------------------
# Конфигурация
# ---------------------------------------------------------------------------

API_BASE = "https://jsonplaceholder.typicode.com"
DEFAULT_OUTPUT_DIR = "./output"


# ---------------------------------------------------------------------------
# Получение данных
# ---------------------------------------------------------------------------


async def fetch_user_data(user_id: int) -> dict:
    """Получает данные пользователя по API."""
    # requests.get() — синхронный HTTP-клиент. Пока он ждёт ответа
    # сервера, поток заблокирован и event loop не может
    # переключиться на другие корутины: aiohttp работает поверх asyncio: во время
    # ожидания сети корутина уступает управление, и запросы идут параллельно.
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{API_BASE}/users/{user_id}") as response:
            response.raise_for_status()
            return await response.json()


async def fetch_user_posts(user_id: int) -> list[dict]:
    """Получает посты пользователя."""
    # исправление тоже, что и в fetch_user_data, requests.get заменён на aiohttp
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{API_BASE}/posts", params={"userId": user_id}) as response:
            response.raise_for_status()
            return await response.json()


# ---------------------------------------------------------------------------
# Файловые операции
# ---------------------------------------------------------------------------

async def save_to_file(data: dict | list, filepath: str) -> None:
    """Сохраняет данные в JSON-файл."""
    # build-in в Python open() и запись через f.write — синхронные
    # aiofiles выполняет файловые операции асинхронно
    # в пуле потоков и отдаёт awaitable-обёртки, так что loop свободен.
    async with aiofiles.open(filepath, "w", encoding="utf-8") as f:
        await f.write(json.dumps(data, indent=2, ensure_ascii=False))


async def load_from_file(filepath: str) -> dict | list:
    """Загружает данные из JSON-файла."""
    # блокирующее синхронное чтение файла заменено на aiofiles - асинхронное чтение.
    async with aiofiles.open(filepath, encoding="utf-8") as f:
        return json.loads(await f.read())


# ---------------------------------------------------------------------------
# Обработка данных
# ---------------------------------------------------------------------------


async def compute_statistics(users: list[dict]) -> dict:
    """Вычисляет статистику по пользователям.

    Имитирует тяжёлые вычисления (агрегация, нормализация).
    """
    def _heavy_checksum(n: int = 10_000_000) -> int:
        """Имитация тяжёлых вычислений (синхронная CPU-bound функция)."""
        total = 0
        for i in range(n):
            total += i * i
        return total

    # цикл не содержит ни одного await, поэтому event loop на ~1 секунду полностью
    # блокируется. asyncio.to_thread (обёртка над loop.run_in_executor) выносит вычисление
    # в отдельный поток, а корутина ждёт результат через await.
    total = await asyncio.to_thread(_heavy_checksum)

    names = sorted(u.get("name", "") for u in users)
    companies = {u.get("company", {}).get("name", "N/A") for u in users}

    return {
        "user_count": len(users),
        "checksum": total,
        "names": names,
        "unique_companies": sorted(companies),
    }


async def enrich_user(user: dict) -> dict:
    """Дополняет профиль пользователя его постами."""
    posts = await fetch_user_posts(user["id"])
    user["posts"] = posts
    user["post_count"] = len(posts)
    return user


# ---------------------------------------------------------------------------
# Служебные функции
# ---------------------------------------------------------------------------


async def wait_for_service(url: str, timeout: float = 2.0) -> bool:
    """Ждёт доступности внешнего сервиса."""
    # time.sleep() усыпляет весь поток вместе с event loop, asyncio.sleep возвращает
    # управление циклу событий, и пока эта корутина «спит», другие работают.
    await asyncio.sleep(timeout)
    return True


async def log_message(message: str, logfile: str = "service.log") -> None:
    """Записывает сообщение в лог-файл."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    # тоже, что и в save_to_file, заменено на aiofiles.
    async with aiofiles.open(logfile, "a", encoding="utf-8") as f:
        await f.write(f"[{timestamp}] {message}\n")


# ---------------------------------------------------------------------------
# Основной пайплайн
# ---------------------------------------------------------------------------


async def process_users(user_ids: list[int], output_dir: str = DEFAULT_OUTPUT_DIR) -> list[dict]:
    """Основной пайплайн: загрузка, обогащение, сохранение."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    await log_message("Pipeline started")

    # Ждём готовности API
    await wait_for_service(API_BASE)

    # Загружаем пользователей параллельно
    tasks = [fetch_user_data(uid) for uid in user_ids]
    users = await asyncio.gather(*tasks)

    await log_message(f"Fetched {len(users)} users")

    # Обогащаем данные постами
    enriched = await asyncio.gather(*(enrich_user(u) for u in users))

    # Сохраняем каждого пользователя
    for user in enriched:
        filepath = f"{output_dir}/user_{user['id']}.json"
        await save_to_file(user, filepath)

    # Считаем статистику
    stats = await compute_statistics(enriched)
    await save_to_file(stats, f"{output_dir}/stats.json")

    await log_message("Pipeline finished")
    return enriched


async def main() -> None:
    user_ids = [1, 2, 3, 4, 5]
    users = await process_users(user_ids)
    print(f"Processed {len(users)} users")
    for user in users:
        print(f"  - {user['name']}: {user['post_count']} posts")


if __name__ == "__main__":
    asyncio.run(main())
