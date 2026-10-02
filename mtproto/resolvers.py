"""Cookie-free share-link resolvers for Diskwala and Terabox.

Both providers are resolved through free public upstream APIs, so the bot needs
no cookies, logins or scraping of its own.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from urllib.parse import quote, urlparse

import aiohttp

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
)

DISKWALA_RESOLVER = os.getenv("DISKWALA_RESOLVER_URL", "https://diskwala-dl-six.vercel.app/api/scrap")
TERABOX_RESOLVER = os.getenv("TERABOX_RESOLVER_URL", "https://disktera.vercel.app/api/download")

DISKWALA_HOSTS = {
    "diskwala.com", "www.diskwala.com",
    "diskwala.net", "www.diskwala.net",
    "diskwala.app", "www.diskwala.app",
}

TERABOX_HOSTS = {
    "terabox.com", "1024terabox.com", "teraboxapp.com", "terabox.app",
    "1024tera.com", "1024tera.cn", "teraboxshare.com", "teraboxlink.com",
    "terasharelink.com", "teraboxurl.com", "terafileshare.com",
    "teraboxdrive.com", "terabox.fun", "freeterabox.com", "tera.app",
    "4funbox.com", "mirrobox.com", "nephobox.com", "momerybox.com", "tibibox.com",
}

# Terabox keeps adding mirror domains that all resolve through the same API,
# so match the family by pattern instead of chasing a hard-coded list.
TERABOX_RE = re.compile(
    r"(^|\.)((\d+)?tera(box)?[a-z0-9-]*|4funbox|mirrobox|nephobox|momerybox|tibibox)\.[a-z.]+$",
    re.IGNORECASE,
)

URL_RE = re.compile(r"https?://[^\s<>\"')\]]+", re.IGNORECASE)


class ResolveError(Exception):
    """Raised when a share link cannot be turned into a direct file link."""


@dataclass
class RemoteFile:
    name: str
    dlink: str
    size_bytes: int = 0
    thumbnail: str = ""
    is_dir: bool = False

    @property
    def pretty_size(self) -> str:
        return format_size(self.size_bytes)


@dataclass
class Resolved:
    provider: str
    share_url: str
    title: str
    files: list[RemoteFile] = field(default_factory=list)


def format_size(num: float) -> str:
    if not num:
        return "unknown"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if num < 1024 or unit == "TB":
            return f"{num:.2f} {unit}" if unit not in {"B", "KB"} else f"{num:.0f} {unit}"
        num /= 1024
    return f"{num:.2f} TB"


def pick_provider(url: str) -> str | None:
    try:
        host = (urlparse(url).hostname or "").lower()
    except ValueError:
        return None
    if not host:
        return None
    if host in DISKWALA_HOSTS or host.removeprefix("www.") in DISKWALA_HOSTS:
        return "diskwala"
    if host in TERABOX_HOSTS or host.removeprefix("www.") in TERABOX_HOSTS:
        return "terabox"
    if TERABOX_RE.search(host):
        return "terabox"
    return None


def extract_links(text: str, limit: int = 3) -> list[tuple[str, str]]:
    """Return [(url, provider)] for supported links found in a message."""
    found: list[tuple[str, str]] = []
    seen: set[str] = set()
    for match in URL_RE.findall(text or ""):
        provider = pick_provider(match)
        if not provider or match in seen:
            continue
        seen.add(match)
        found.append((match, provider))
        if len(found) >= limit:
            break
    return found


def download_headers(share_url: str) -> dict[str, str]:
    """Browser-like headers; both CDNs reject bare cloud requests."""
    headers = {
        "User-Agent": UA,
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    parsed = urlparse(share_url)
    if parsed.scheme in {"http", "https"}:
        headers["Referer"] = share_url
    return headers


async def _get_json(session: aiohttp.ClientSession, url: str, timeout: int, label: str) -> dict:
    try:
        async with session.get(
            url,
            headers={"User-Agent": UA, "Accept": "application/json"},
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as response:
            if response.status != 200:
                raise ResolveError(f"{label} resolver failed (HTTP {response.status})")
            return await response.json(content_type=None)
    except aiohttp.ClientError as exc:
        raise ResolveError(f"{label} resolver unreachable ({exc.__class__.__name__})") from exc


async def _resolve_diskwala(session, share_url: str, timeout: int) -> Resolved:
    payload = await _get_json(
        session, f"{DISKWALA_RESOLVER}?q={quote(share_url, safe='')}", timeout, "Diskwala"
    )
    file = ((payload or {}).get("data") or {}).get("file") or {}
    if not payload.get("success") or not file.get("downloadUrl"):
        raise ResolveError(
            "Could not resolve this Diskwala link - it may be private, deleted, or a playlist."
        )
    ext = re.sub(r"[^a-z0-9]", "", str(file.get("extension") or "mp4").lower()) or "mp4"
    name = str(file.get("name") or "diskwala").strip() or "diskwala"
    if not name.lower().endswith(f".{ext}"):
        name = f"{name}.{ext}"
    return Resolved(
        provider="diskwala",
        share_url=share_url,
        title=name,
        files=[
            RemoteFile(
                name=name,
                dlink=file["downloadUrl"],
                size_bytes=int(file.get("size") or 0),
                thumbnail=str(file.get("thumb") or ""),
            )
        ],
    )


def _terabox_entries(payload: dict) -> list[dict]:
    data = payload.get("data")
    for candidate in (
        (data or {}).get("data") if isinstance(data, dict) else None,
        (data or {}).get("list") if isinstance(data, dict) else None,
        (data or {}).get("files") if isinstance(data, dict) else None,
        data,
        payload.get("list"),
        payload.get("files"),
    ):
        if isinstance(candidate, list) and candidate:
            return candidate
    if isinstance(data, dict) and (data.get("downloadLink") or data.get("dlink")):
        return [data]
    return []


async def _resolve_terabox(session, share_url: str, timeout: int) -> Resolved:
    payload = await _get_json(
        session, f"{TERABOX_RESOLVER}?link={quote(share_url, safe='')}", timeout, "Terabox"
    )
    files: list[RemoteFile] = []
    for index, entry in enumerate(_terabox_entries(payload)):
        if not isinstance(entry, dict):
            continue
        dlink = entry.get("downloadLink") or entry.get("download_link") or entry.get("dlink") or ""
        if not dlink or entry.get("isDir") or entry.get("is_dir"):
            continue
        name = str(
            entry.get("fileName") or entry.get("server_filename") or entry.get("name") or ""
        ).strip() or f"terabox-file-{index + 1}"
        size = int(entry.get("fileSize") or entry.get("size") or 0)
        files.append(
            RemoteFile(
                name=name,
                dlink=dlink,
                size_bytes=size,
                thumbnail=str(entry.get("thumbnail") or entry.get("thumb") or ""),
            )
        )
    if not files:
        raise ResolveError(
            "Could not resolve this Terabox link - it may be private, deleted, "
            "password protected, or a folder with no direct files."
        )
    return Resolved(provider="terabox", share_url=share_url, title=files[0].name, files=files)


async def resolve(session: aiohttp.ClientSession, share_url: str, timeout: int = 30) -> Resolved:
    provider = pick_provider(share_url)
    if provider == "diskwala":
        return await _resolve_diskwala(session, share_url, timeout)
    if provider == "terabox":
        return await _resolve_terabox(session, share_url, timeout)
    raise ResolveError("Unsupported link. Send a Diskwala or Terabox share link.")
