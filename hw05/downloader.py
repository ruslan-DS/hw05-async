"""Асинхронная загрузка файлов с ограничением параллельности."""

import asyncio
import hashlib
from pathlib import Path

import aiofiles
import aiohttp
from tqdm.asyncio import tqdm

CHUNK_SIZE = 64 * 1024
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)


def _filename_for(url: str) -> str:
    """Последний сегмент пути URL; если его нет — md5-хеш URL."""
    path = url.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    name = path.rsplit("/", 1)[-1]
    if not name or "://" in name:
        name = hashlib.md5(url.encode()).hexdigest()
    return name


async def _download_one(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    url: str,
    dest_dir: Path,
) -> bool:
    """Скачивает один файл. Возвращает True при успехе, False при любой ошибке."""
    target = dest_dir / _filename_for(url)
    # не больше max_concurrent корутин одновременно внутри
    async with semaphore:
        try:
            async with session.get(url, timeout=REQUEST_TIMEOUT) as response:
                response.raise_for_status()  # 4xx/5xx -> ClientResponseError
                async with aiofiles.open(target, "wb") as f:
                    async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                        await f.write(chunk)
            return True
        except (aiohttp.ClientError, TimeoutError, OSError):
            target.unlink(missing_ok=True)
            return False


async def download_all(
    urls: list[str],
    dest_dir: str | Path,
    max_concurrent: int = 5,
) -> dict[str, bool]:
    """Скачивает файлы по URL и сохраняет в dest_dir.

    Параметры:
        urls: список URL для загрузки
        dest_dir: директория для сохранения файлов
        max_concurrent: максимальное число одновременных загрузок

    Возвращает:
        dict[str, bool] — URL -> True (успех) / False (ошибка)

    Требования:
        - Использовать aiohttp для HTTP-запросов
        - Ограничить параллельность через asyncio.Semaphore
        - Обработать HTTP-ошибки и таймауты (вернуть False)
        - Показывать прогресс через tqdm
        - Имя файла — последний сегмент URL (или хеш, если нет имени)
    """
    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)
    if not urls:
        return {}

    semaphore = asyncio.Semaphore(max_concurrent)
    async with aiohttp.ClientSession() as session:
        tasks = [_download_one(session, semaphore, url, dest) for url in urls]
        results = await tqdm.gather(*tasks, desc="Downloading", unit="file")

    return dict(zip(urls, results))
