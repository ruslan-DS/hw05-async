"""Асинхронный пайплайн обработки данных."""

from collections.abc import AsyncIterator
from pathlib import Path

import aiofiles

# Конфигурация вариантов: формат входных данных
VARIANT_SOURCES = {
    0: "server_log",  # [2024-01-15 10:23:45] ERROR: Connection timeout
    1: "json_events",  # {"event": "click", "ts": 1705312345, "user_id": "abc"}
    2: "csv_metrics",  # 2024-01-15T10:23:45,cpu=78.5,mem=4096,disk=85.2
}


async def read_chunks(path: str | Path, chunk_size: int = 8192) -> AsyncIterator[bytes]:
    """Читает файл асинхронно чанками заданного размера.

    Параметры:
        path: путь к файлу
        chunk_size: размер чанка в байтах

    Yields:
        bytes — очередной чанк данных
    """
    async with aiofiles.open(path, "rb") as f:
        while chunk := await f.read(chunk_size):
            yield chunk


async def parse_lines(chunks: AsyncIterator[bytes]) -> AsyncIterator[str]:
    """Собирает строки из потока байтовых чанков.

    Чанк может содержать неполную строку — нужно буферизовать остаток
    и склеивать с началом следующего чанка.

    Параметры:
        chunks: асинхронный итератор байтовых чанков

    Yields:
        str — очередная полная строка (без \\n)
    """
    buffer = b""
    async for chunk in chunks:
        buffer += chunk
        # всё до последнего "\n" — готовые строки, хвост после него — в буфер
        *lines, buffer = buffer.split(b"\n")
        for line in lines:
            yield line.decode("utf-8")
    if buffer:  # файл без завершающего "\n"
        yield buffer.decode("utf-8")


async def filter_lines(lines: AsyncIterator[str], pattern: str) -> AsyncIterator[str]:
    """Фильтрует строки по подстроке или регулярному выражению.

    Параметры:
        lines: асинхронный итератор строк
        pattern: подстрока для поиска

    Yields:
        str — строки, содержащие pattern
    """
    async for line in lines:
        if pattern in line:
            yield line


async def batch(items: AsyncIterator[str], size: int) -> AsyncIterator[list[str]]:
    """Группирует элементы в батчи заданного размера.

    Последний батч может быть меньше size.

    Параметры:
        items: асинхронный итератор элементов
        size: размер батча

    Yields:
        list[str] — очередной батч элементов
    """
    current: list[str] = []
    async for item in items:
        current.append(item)
        if len(current) == size:
            yield current
            current = []
    if current:
        yield current
