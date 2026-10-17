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
from contextlib import asynccontextmanager

import aiohttp
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait, MessageNotModified
from pyrogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

import health
from telegraph import MAX_LINKS, PageError, fetch_article, normalize_share, page_links
from page_batch import BatchStats, MAX_FILES, MAX_PENDING_BATCHES, run_batch
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
batch_lock = asyncio.Lock()
batch_jobs: dict[int, asyncio.Task] = {}
# Short-lived cache so inline buttons stay under Telegram's 64-byte callback cap.
cache: dict[str, tuple[float, Resolved]] = {}
CACHE_TTL = 3600


@asynccontextmanager
async def transfer_slot():
    health.STATE["queued_jobs"] += 1
    acquired = False
    try:
        await job_slot.acquire()
        acquired = True
        health.STATE["queued_jobs"] -= 1
        health.STATE["active_jobs"] += 1
        yield
    finally:
        if acquired:
            health.STATE["active_jobs"] -= 1
            job_slot.release()
        else:
            health.STATE["queued_jobs"] -= 1


def batch_summary(stats: BatchStats, total: int, cancelled: bool = False) -> str:
    title = "Batch cancelled" if cancelled else "Batch complete"
    return (f"<b>{title}</b>\n"
            f"Links processed: {stats.links}/{total}\n"
            f"Sent: {stats.sent} · Skipped/too large: {stats.skipped}\n"
            f"Failed files: {stats.failed} · Failed links: {stats.link_failures}"
            + (f"\nStopped at the {MAX_FILES}-file safety limit." if stats.limited else ""))


async def process_pages(message: Message, pages: list[str], direct: list[str]) -> None:
    status = None
    stats = BatchStats()
    links: list[str] = []
    page_failures = 0
    try:
        status = await message.reply_text(
            "<b>Graph/Telegraph batch queued</b>\nPages are processed one at a time. Use /cancel to stop.",
            disable_web_page_preview=True,
        )
        async with batch_lock:
            await safe_edit(status, "<b>Scanning Graph/Telegraph article links…</b>")
            seen: set[str] = set()
            for page in pages:
                try:
                    extracted = await fetch_article(http, page, config.request_timeout)
                except PageError as exc:
                    page_failures += 1
                    await message.reply_text(f"<b>Page scan failed:</b> {html_escape(str(exc))}", disable_web_page_preview=True)
                    continue
                for url in extracted:
                    if url not in seen:
                        seen.add(url)
                        links.append(url)
            for raw in direct:
                url = normalize_share(raw)
                if url and url not in seen:
                    seen.add(url)
                    links.append(url)
            if len(links) > MAX_LINKS:
                await safe_edit(status, f"This batch contains {len(links)} links; the limit is {MAX_LINKS}. Send fewer pages.")
                return
            if not links:
                await safe_edit(status, "<b>No supported share links were found.</b> The article must contain public Diskwala/Terabox links.")
                return
            await message.reply_text(
                f"<b>Found {len(links)} unique share links.</b>\nDownloading and sending files one by one. Use /cancel to stop.",
                disable_web_page_preview=True,
            )

            async def resolve_link(url):
                return await resolve(http, url, config.request_timeout)

            async def send_file(result, item, index):
                return await deliver(message, result, item, index)

            async def progress(number, total, current):
                await safe_edit(status, f"<b>Batch: link {number}/{total}</b>\nSent: {current.sent} · Failed links: {current.link_failures}\nFiles are sent sequentially. Use /cancel to stop.")

            await run_batch(links, resolve_link, send_file, progress, stats)
            await safe_edit(status, batch_summary(stats, len(links)) + f"\nFailed page scans: {page_failures}")
    except asyncio.CancelledError:
        if status:
            await safe_edit(status, batch_summary(stats, len(links), cancelled=True))
        raise
    except Exception:
        log.exception("page batch failed")
        if status:
            await safe_edit(status, "<b>Batch stopped unexpectedly.</b> Already sent files are unaffected. Please retry the remaining links.")
    finally:
        if batch_jobs.get(message.chat.id) is asyncio.current_task():
            batch_jobs.pop(message.chat.id, None)
        health.STATE["pending_batches"] = len(batch_jobs)


def forget_batch(chat_id: int, task: asyncio.Task) -> None:
    # A task cancelled before its first coroutine step never reaches its finally.
    if batch_jobs.get(chat_id) is task:
        batch_jobs.pop(chat_id, None)
    health.STATE["pending_batches"] = len(batch_jobs)


def message_link_text(message: Message) -> str:
    # Telegram text_link entities contain URLs that are absent from visible text.
    hidden = [entity.url for entity in [*(message.entities or []), *(message.caption_entities or [])]
              if getattr(entity, "url", None)]
    return "\n".join([message.text or message.caption or "", *hidden])


@app.on_message(filters.command("cancel") & filters.private)
async def on_cancel(_client: Client, message: Message) -> None:
    if not allowed(message.from_user.id):
        return
    task = batch_jobs.get(message.chat.id)
    if task and not task.done():
        task.cancel()
        await message.reply_text("Cancelling your Graph/Telegraph batch. Already sent files will stay in the chat.")
    else:
        await message.reply_text("You have no active Graph/Telegraph batch.")


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
        "Graph/Telegraph page \u2192 scan and send every file sequentially\n"
        "Use /cancel to stop a page batch (up to 50 links / 100 files).\n"
        f"\U0001f4e6 Files up to <b>{config.max_file_mb} MB</b> land right here in chat\n"
        "\U0001f3a5 Videos arrive as playable video, music as audio\n"
        "\U0001f517 Anything larger \u2192 direct download link\n\n"
        "<i>Powered by MTProto \u2014 no 50 MB limit.</i>",
        disable_web_page_preview=True,
    )


@app.on_message((filters.text | filters.caption) & filters.private & ~filters.command(["start", "help", "cancel"]))
async def on_link(_client: Client, message: Message) -> None:
    if not allowed(message.from_user.id):
        await message.reply_text("\U0001f512 <b>Private bot</b>\n<blockquote>Access denied.</blockquote>")
        return

    text = message_link_text(message)
    try:
        pages = page_links(text)
    except PageError as exc:
        await message.reply_text(html_escape(str(exc)))
        return
    if pages:
        existing = batch_jobs.get(message.chat.id)
        if existing and not existing.done():
            await message.reply_text("You already have a page batch running. Use /cancel before starting another.")
            return
        if len(batch_jobs) >= MAX_PENDING_BATCHES:
            await message.reply_text("The page-batch queue is full. Please try again later.")
            return
        direct = [url for url, _ in extract_links(text, limit=MAX_LINKS + 1)]
        task = asyncio.create_task(process_pages(message, pages, direct))
        batch_jobs[message.chat.id] = task
        task.add_done_callback(lambda done: forget_batch(message.chat.id, done))
        health.STATE["pending_batches"] = len(batch_jobs)
        return

    links = extract_links(text)
    if not links:
        await message.reply_text(
            "\U0001f914 <b>No supported link found there.</b>\n\n"
            "Send a Graph/Telegraph article, Diskwala or Terabox link, e.g.\n"
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
    await deliver(query.message, result, item, index)


async def deliver(message: Message, result: Resolved, item, index: int) -> str:
    if item.size_bytes and item.size_bytes > config.max_bytes:
        await message.reply_text(f"<b>Too large:</b> {html_escape(item.name)} ({item.pretty_size}).\n<code>{html_escape(item.dlink)}</code>", disable_web_page_preview=True)
        return "skipped"
    status = await message.reply_text(
        f"\u2b07\ufe0f <b>Starting download\u2026</b>\n<code>{html_escape(item.name)}</code>"
    )

    if job_slot.locked():
        await safe_edit(status, "\u23f3 <b>Another transfer is running \u2014 you're next in queue\u2026</b>")

    async with transfer_slot():  # shared by buttons and automatic page batches
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

            if fresh.size_bytes and fresh.size_bytes > config.max_bytes:
                await safe_edit(status, f"<b>Too large:</b> {html_escape(fresh.name)}\n<code>{html_escape(fresh.dlink)}</code>")
                return "skipped"
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
            target = message.chat.id
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
            return "sent"
        except DownloadError as exc:
            if exc.too_big:
                await safe_edit(
                    status,
                    f"\u26a0\ufe0f <b>File exceeds {config.max_file_mb} MB.</b>\n\n"
                    f"<code>{html_escape(item.dlink)}</code>",
                )
                return "skipped"
            else:
                await safe_edit(status, f"\u274c <b>{html_escape(str(exc))}</b>\n\n<code>{html_escape(item.dlink)}</code>")
        except FloodWait as exc:
            await safe_edit(status, f"\u23f3 <b>Telegram rate limit \u2014 wait {exc.value}s and retry.</b>")
            await asyncio.sleep(exc.value)
        except Exception as exc:
            log.exception("delivery failed")
            await safe_edit(status, f"\u274c <b>Failed:</b> {html_escape(exc.__class__.__name__)}")
        finally:
            # Cancellation also releases the slot and cleans up the staged file.
            cleanup(path)
        return "failed"


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
        for task in list(batch_jobs.values()):
            task.cancel()
        if batch_jobs:
            await asyncio.gather(*list(batch_jobs.values()), return_exceptions=True)
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
