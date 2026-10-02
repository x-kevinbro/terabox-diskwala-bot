"""Diskwala + Terabox downloader bot on MTProto (Pyrogram API).

Why MTProto: the HTTP Bot API caps uploads at 50 MB. Talking to Telegram over
MTProto with API_ID/API_HASH raises that to 2000 MB per file.

Memory strategy for a 512 MB instance:
  * downloads stream to disk in 1 MiB chunks (downloader.py) - never buffered
  * uploads stream from disk in small chunks by the MTProto client
  * one transfer at a time (semaphore), so peak RSS stays ~60-90 MB
  * temp files are deleted in a finally block, even on error or cancellation
"""
from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

import aiohttp
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

import health
from config import config
from downloader import DownloadError, cleanup, download_to_disk
from resolvers import (
    ResolveError,
    Resolved,
    download_headers,
    extract_links,
    format_size,
    resolve,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bot")
logging.getLogger("pyrogram").setLevel(logging.WARNING)

VIDEO_EXT = {"mp4", "mkv", "webm", "mov", "m4v", "avi", "mpg", "mpeg", "ts"}
AUDIO_EXT = {"mp3", "m4a", "flac", "wav", "ogg", "aac", "opus", "wma"}
PROVIDER_STYLE = {"diskwala": ("\U0001f3ac", "DISKWALA"), "terabox": ("\U0001f4e6", "TERABOX")}
DIVIDER = "\u2504" * 14

config.workdir.mkdir(parents=True, exist_ok=True)
config.download_dir.mkdir(parents=True, exist_ok=True)

# Pyrogram binds to whatever loop exists when the Client is built. Create and
# install that loop here so the bot, aiohttp session and health server all
# share one loop (asyncio.run() would make a second one and crash).
LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(LOOP)

app = Client(
    "diskwala-terabox-bot",
    api_id=config.api_id,
    api_hash=config.api_hash,
    bot_token=config.bot_token,
    workdir=str(config.workdir),
    parse_mode=ParseMode.HTML,
    # Small worker pool: more workers would mean more parallel transfers and
    # more memory pressure on a 512 MB box.
    workers=2,
    in_memory=True,
)

job_slot = asyncio.Semaphore(config.max_concurrent_jobs)
http: aiohttp.ClientSession | None = None
# Short-lived cache so inline buttons stay under Telegram's 64-byte callback cap.
cache: dict[str, tuple[float, Resolved]] = {}
CACHE_TTL = 3600


def remember(result: Resolved) -> str:
    now = time.time()
    for key in [k for k, (ts, _) in cache.items() if now - ts > CACHE_TTL]:
        cache.pop(key, None)
    key = f"{int(now * 1000) % 10_000_000:07d}"
    cache[key] = (now, result)
    return key


def ext_of(name: str) -> str:
    return Path(name).suffix.lstrip(".").lower()


def file_emoji(name: str) -> str:
    ext = ext_of(name)
    if ext in VIDEO_EXT:
        return "\U0001f3ac"
    if ext in AUDIO_EXT:
        return "\U0001f3b5"
    if ext in {"jpg", "jpeg", "png", "webp", "gif"}:
        return "\U0001f5bc\ufe0f"
    if ext in {"zip", "rar", "7z", "tar", "gz"}:
        return "\U0001f5dc\ufe0f"
    return "\U0001f4c4"


def allowed(user_id: int) -> bool:
    if config.public_bot:
        return True
    return user_id in config.allowed_users if config.allowed_users else True


def bar(done: int, total: int) -> str:
    if not total:
        return "\u2593" * 2 + "\u2591" * 10
    filled = max(0, min(12, int(done / total * 12)))
    return "\u2593" * filled + "\u2591" * (12 - filled)


def info_payload(result: Resolved, key: str) -> tuple[str, InlineKeyboardMarkup | None]:
    icon, label = PROVIDER_STYLE.get(result.provider, ("\U0001f4e6", result.provider.upper()))
    files = [f for f in result.files if not f.is_dir][:10]
    lines = [
        f"{icon} <b><u>{label} \u2014 LINK READY</u></b>",
        "",
        f"<code>\u258e\u2705 {len(files)} file{'' if len(files) == 1 else 's'} resolved</code>",
        "",
        DIVIDER,
    ]
    for index, item in enumerate(files):
        too_big = item.size_bytes > config.max_bytes
        lines.append(f"{file_emoji(item.name)} {html_escape(item.name)}")
        suffix = "  \u2022  \u26a0\ufe0f <i>over 2 GB \u2014 link only</i>" if too_big else ""
        lines.append(f"\U0001f4be {item.pretty_size}{suffix}")
        if index < len(files) - 1:
            lines.append("")
    lines += [
        DIVIDER,
        "",
        "<b>\U0001f447 Pick an action:</b>",
        "",
        "<blockquote>\u2b07\ufe0f <b>Download</b> \u2014 file lands right here in chat\n"
        "\U0001f517 <b>Direct link</b> \u2014 raw URL, tap to copy</blockquote>",
    ]
    keyboard = [
        [
            InlineKeyboardButton(
                f"\u2b07\ufe0f Download{f' #{i + 1}' if len(files) > 1 else ''}",
                callback_data=f"dl:{key}:{i}",
            ),
            InlineKeyboardButton(
                f"\U0001f517{f' #{i + 1}' if len(files) > 1 else ' Direct Link'}",
                callback_data=f"ln:{key}:{i}",
            ),
        ]
        for i in range(len(files))
    ]
    return "\n".join(lines), InlineKeyboardMarkup(keyboard) if keyboard else None


def html_escape(text: str) -> str:
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def safe_edit(message: Message, text: str) -> None:
    try:
        await message.edit_text(text, disable_web_page_preview=True)
    except MessageNotModified:
        pass
    except FloodWait as exc:
        await asyncio.sleep(exc.value)
    except Exception as exc:  # editing is cosmetic; never kill a transfer for it
        log.debug("edit failed: %s", exc)


@app.on_message(filters.command(["start", "help"]) & filters.private)
async def on_start(_client: Client, message: Message) -> None:
    if not allowed(message.from_user.id):
        await message.reply_text("\U0001f512 <b>Private bot</b>\n<blockquote>Access denied.</blockquote>")
        return
    await message.reply_text(
        "\U0001f44b <b>Welcome to the Diskwala + Terabox Downloader!</b>\n\n"
        "<blockquote>\U0001f517 Send me a Diskwala or Terabox share link and I'll fetch the file.</blockquote>\n\n"
        "<b>What I do:</b>\n"
        "\U0001f3ac Diskwala &amp; \U0001f4e6 Terabox links \u2192 info + downloads\n"
        f"\U0001f4e6 Files up to <b>{config.max_file_mb} MB</b> land right here in chat\n"
        "\U0001f3a5 Videos arrive as playable video, music as audio\n"
        "\U0001f517 Anything larger \u2192 direct download link\n\n"
        "<i>Powered by MTProto \u2014 no 50 MB limit.</i>",
        disable_web_page_preview=True,
    )


@app.on_message(filters.text & filters.private & ~filters.command(["start", "help"]))
async def on_link(_client: Client, message: Message) -> None:
    if not allowed(message.from_user.id):
        await message.reply_text("\U0001f512 <b>Private bot</b>\n<blockquote>Access denied.</blockquote>")
        return

    links = extract_links(message.text or message.caption or "")
    if not links:
        await message.reply_text(
            "\U0001f914 <b>No supported link found there.</b>\n\n"
            "Send a Diskwala or Terabox link, e.g.\n"
            "<code>https://diskwala.com/app/xxxx</code>\n"
            "<code>https://teraboxshare.com/s/xxxx</code>",
            disable_web_page_preview=True,
        )
        return

    for share_url, provider in links:
        status = await message.reply_text(
            f"\U0001f50d <b>Resolving {provider.title()} link\u2026</b>", disable_web_page_preview=True
        )
        try:
            result = await resolve(http, share_url, config.request_timeout)
        except ResolveError as exc:
            await safe_edit(status, f"\u274c <b>{html_escape(str(exc))}</b>")
            continue
        except Exception as exc:
            log.exception("resolve crashed")
            await safe_edit(status, f"\u274c <b>Resolver error:</b> {html_escape(exc.__class__.__name__)}")
            continue

        text, keyboard = info_payload(result, remember(result))
        try:
            await status.edit_text(text, reply_markup=keyboard, disable_web_page_preview=True)
        except Exception:
            await message.reply_text(text, reply_markup=keyboard, disable_web_page_preview=True)


@app.on_callback_query()
async def on_button(_client: Client, query: CallbackQuery) -> None:
    if not allowed(query.from_user.id):
        await query.answer("Access denied.", show_alert=True)
        return

    try:
        action, key, index_raw = (query.data or "").split(":", 2)
        index = int(index_raw)
    except ValueError:
        await query.answer("Malformed button.", show_alert=True)
        return

    entry = cache.get(key)
    if not entry:
        await query.answer("This link expired - please send it again.", show_alert=True)
        return
    result = entry[1]
    files = [f for f in result.files if not f.is_dir]
    if index >= len(files):
        await query.answer("File not found.", show_alert=True)
        return
    item = files[index]

    if action == "ln":
        await query.answer()
        await query.message.reply_text(
            f"\U0001f517 <b>Direct link \u2014 {html_escape(item.name)}</b>\n\n"
            f"<code>{html_escape(item.dlink)}</code>\n\n"
            "<i>Tap to copy. Links are short-lived.</i>",
            disable_web_page_preview=True,
        )
        return

    if item.size_bytes and item.size_bytes > config.max_bytes:
        await query.answer()
        await query.message.reply_text(
            f"\u26a0\ufe0f <b>{html_escape(item.name)}</b> is {item.pretty_size}, above the "
            f"{config.max_file_mb} MB Telegram limit.\n\n<code>{html_escape(item.dlink)}</code>",
            disable_web_page_preview=True,
        )
        return

    await query.answer("Queued\u2026")
    await deliver(query, result, item, index)


async def deliver(query: CallbackQuery, result: Resolved, item, index: int) -> None:
    status = await query.message.reply_text(
        f"\u2b07\ufe0f <b>Starting download\u2026</b>\n<code>{html_escape(item.name)}</code>"
    )

    if job_slot.locked():
        await safe_edit(status, "\u23f3 <b>Another transfer is running \u2014 you're next in queue\u2026</b>")

    async with job_slot:  # one transfer at a time: protects RAM and disk
        health.STATE["active_jobs"] = config.max_concurrent_jobs - job_slot._value
        path: Path | None = None
        try:
            # Links can expire between resolve and button press; refresh first.
            fresh = item
            try:
                refreshed = await resolve(http, result.share_url, config.request_timeout)
                candidates = [f for f in refreshed.files if not f.is_dir]
                if index < len(candidates):
                    fresh = candidates[index]
            except Exception as exc:
                log.info("refresh failed, using cached link: %s", exc)

            headers = download_headers(result.share_url)
            started = time.monotonic()

            async def on_download(done: int, total: int) -> None:
                speed = done / max(0.1, time.monotonic() - started) / 1024 / 1024
                pct = f"{done / total * 100:.1f}%" if total else format_size(done)
                await safe_edit(
                    status,
                    f"\u2b07\ufe0f <b>Downloading\u2026</b>\n<code>{html_escape(fresh.name)}</code>\n\n"
                    f"<code>{bar(done, total)}</code> {pct}\n"
                    f"\U0001f4be {format_size(done)}"
                    + (f" / {format_size(total)}" if total else "")
                    + f"\n\u26a1 {speed:.2f} MB/s",
                )

            try:
                path = await download_to_disk(http, fresh.dlink, headers, fresh.name, on_download)
            except DownloadError as exc:
                if exc.retryable:
                    await safe_edit(status, "\U0001f501 <b>Upstream hiccup \u2014 retrying once\u2026</b>")
                    await asyncio.sleep(2)
                    path = await download_to_disk(http, fresh.dlink, headers, fresh.name, on_download)
                else:
                    raise

            size = path.stat().st_size
            await safe_edit(
                status,
                f"\u2b06\ufe0f <b>Uploading to Telegram\u2026</b>\n<code>{html_escape(fresh.name)}</code>\n\n"
                f"\U0001f4be {format_size(size)}",
            )

            up_started = time.monotonic()
            last = [0.0]

            async def on_upload(current: int, total: int) -> None:
                now = time.monotonic()
                if now - last[0] < config.progress_interval:
                    return
                last[0] = now
                speed = current / max(0.1, now - up_started) / 1024 / 1024
                await safe_edit(
                    status,
                    f"\u2b06\ufe0f <b>Uploading\u2026</b>\n<code>{html_escape(fresh.name)}</code>\n\n"
                    f"<code>{bar(current, total)}</code> "
                    f"{current / total * 100:.1f}%\n\u26a1 {speed:.2f} MB/s",
                )

            caption = (
                f"{file_emoji(fresh.name)} <b>{html_escape(fresh.name)}</b>\n"
                f"\U0001f4be {format_size(size)}  \u2022  "
                f"{PROVIDER_STYLE.get(result.provider, ('', result.provider.upper()))[1]}"
            )
            kind = (
                "video"
                if ext_of(fresh.name) in VIDEO_EXT
                else "audio"
                if ext_of(fresh.name) in AUDIO_EXT
                else "document"
            )
            target = query.message.chat.id
            # Pyrogram streams the file from disk, so RAM stays flat here too.
            if kind == "video":
                await app.send_video(
                    target,
                    video=str(path),
                    caption=caption,
                    file_name=fresh.name,
                    supports_streaming=True,
                    progress=on_upload,
                )
            elif kind == "audio":
                await app.send_audio(
                    target, audio=str(path), caption=caption, file_name=fresh.name, progress=on_upload
                )
            else:
                await app.send_document(
                    target,
                    document=str(path),
                    caption=caption,
                    file_name=fresh.name,
                    force_document=True,
                    progress=on_upload,
                )

            await safe_edit(status, f"\u2705 <b>Sent!</b>\n<code>{html_escape(fresh.name)}</code>")
        except DownloadError as exc:
            if exc.too_big:
                await safe_edit(
                    status,
                    f"\u26a0\ufe0f <b>File exceeds {config.max_file_mb} MB.</b>\n\n"
                    f"<code>{html_escape(item.dlink)}</code>",
                )
            else:
                await safe_edit(status, f"\u274c <b>{html_escape(str(exc))}</b>\n\n<code>{html_escape(item.dlink)}</code>")
        except FloodWait as exc:
            await safe_edit(status, f"\u23f3 <b>Telegram rate limit \u2014 wait {exc.value}s and retry.</b>")
        except Exception as exc:
            log.exception("delivery failed")
            await safe_edit(status, f"\u274c <b>Failed:</b> {html_escape(exc.__class__.__name__)}")
        finally:
            # Always free the disk immediately, success or failure.
            cleanup(path)
            health.STATE["active_jobs"] = config.max_concurrent_jobs - job_slot._value


async def main() -> None:
    global http
    missing = config.missing()
    runner = await health.start_health_server()
    if missing:
        health.STATE["bot"] = f"missing credentials: {', '.join(missing)}"
        log.error("Missing required environment variables: %s", ", ".join(missing))
        log.error("Set them on Koyeb (Service -> Settings -> Environment variables).")
        while True:  # keep the health endpoint up so the platform shows the cause
            await asyncio.sleep(60)

    http = aiohttp.ClientSession(trust_env=True)
    try:
        await app.start()
        me = await app.get_me()
        health.STATE["bot"] = f"@{me.username}"
        log.info("Logged in as @%s (id %s)", me.username, me.id)
        log.info("MTProto build | max file %s MB | chunk %s KB", config.max_file_mb, config.chunk_bytes // 1024)
        log.info("Providers: diskwala, terabox | public=%s", config.public_bot)
        await asyncio.Event().wait()
    finally:
        health.STATE["bot"] = "stopped"
        await http.close()
        try:
            await app.stop()
        except Exception:
            pass
        await runner.cleanup()


if __name__ == "__main__":
    try:
        LOOP.run_until_complete(main())
    except KeyboardInterrupt:
        pass
