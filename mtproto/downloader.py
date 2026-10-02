"""Chunked, memory-safe downloader.

The whole point of this module is that a 2 GB file never lands in RAM: bytes
arrive in 1 MiB chunks and go straight to disk, so resident memory stays flat
and the 512 MB Koyeb instance cannot OOM.
"""
from __future__ import annotations

import os
import shutil
import time
import uuid
from pathlib import Path

import aiohttp

from config import config

RETRYABLE_STATUS = {403, 408, 429, 500, 502, 503, 504}


class DownloadError(Exception):
    def __init__(self, message: str, retryable: bool = False, too_big: bool = False):
        super().__init__(message)
        self.retryable = retryable
        self.too_big = too_big


def free_disk_bytes(path: Path) -> int:
    target = path if path.exists() else path.parent
    try:
        return shutil.disk_usage(target).free
    except OSError:
        return 0


async def download_to_disk(
    session: aiohttp.ClientSession,
    url: str,
    headers: dict[str, str],
    filename: str,
    on_progress=None,
) -> Path:
    """Stream `url` to a temp file and return its path. Caller must delete it."""
    config.download_dir.mkdir(parents=True, exist_ok=True)
    suffix = Path(filename).suffix
    target = config.download_dir / f"dl_{uuid.uuid4().hex[:10]}{suffix}"

    try:
        response = await session.get(
            url,
            headers=headers,
            allow_redirects=True,
            # Only the response headers are time-limited. A slow but healthy
            # transfer must never be aborted mid-stream.
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=config.request_timeout, sock_read=120),
        )
    except aiohttp.ClientError as exc:
        raise DownloadError(f"download connection failed ({exc.__class__.__name__})", retryable=True) from exc

    async with response:
        if response.status not in (200, 206):
            raise DownloadError(
                f"download failed (HTTP {response.status})",
                retryable=response.status in RETRYABLE_STATUS,
            )

        total = int(response.headers.get("content-length") or 0)
        if total and total > config.max_bytes:
            raise DownloadError("too_big", too_big=True)
        if total and total + (64 * 1024 * 1024) > free_disk_bytes(config.download_dir):
            raise DownloadError("not enough free disk space on this instance to stage the file")

        received = 0
        last_tick = 0.0
        try:
            with target.open("wb") as handle:
                async for chunk in response.content.iter_chunked(config.chunk_bytes):
                    handle.write(chunk)
                    received += len(chunk)
                    if received > config.max_bytes:
                        raise DownloadError("too_big", too_big=True)
                    if on_progress:
                        now = time.monotonic()
                        if now - last_tick >= config.progress_interval:
                            last_tick = now
                            await on_progress(received, total)
        except DownloadError:
            cleanup(target)
            raise
        except (aiohttp.ClientError, OSError) as exc:
            cleanup(target)
            raise DownloadError(f"transfer interrupted ({exc.__class__.__name__})", retryable=True) from exc

    return target


def cleanup(*paths: Path | None) -> None:
    """Delete temp files immediately - disk on a free instance is tiny."""
    for path in paths:
        if not path:
            continue
        try:
            os.unlink(path)
        except OSError:
            pass
