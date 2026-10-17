"""Archive-first delivery with a durable queue and conservative upload recovery."""
from __future__ import annotations

import asyncio
import hashlib
import logging
import secrets
import time
from pathlib import Path

from pyrogram.errors import FloodWait
from pyrogram.enums import ParseMode
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from downloader import DownloadError, cleanup, download_to_disk
from resolvers import download_headers, resolve
from storage import ReviewRequired, StorageError

log = logging.getLogger('archive')


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def file_slot(item, index):
    return hashlib.sha256(f'{index}\0{item.name}\0{item.size_bytes}'.encode()).hexdigest()


class ArchiveService:
    def __init__(self, ledger, client, session, config, transfer_slot, delivery_username):
        self.ledger, self.client, self.http, self.config = ledger, client, session, config
        self.transfer_slot, self.delivery_username = transfer_slot, delivery_username.lstrip('@')
        self.owner = secrets.token_hex(16)
        self.wake = asyncio.Event()
        self.current = None
        self.active_task = None
        self.worker_task = None
        self.lease_lost = False

    def button(self, record):
        return InlineKeyboardMarkup([[InlineKeyboardButton('Get video',
            url=f'https://t.me/{self.delivery_username}?start={record["token"]}')]])

    async def submit(self, message, links, source_page=''):
        records = await self.ledger.enqueue(links, message.chat.id, source_page)
        new = sum(kind != 'duplicate' for _, kind in records)
        for record, kind in records:
            if kind == 'duplicate':
                if record['state'] == 'completed':
                    # Only videos already stored in the database get the companion-bot link.
                    await message.reply_text('✅ Already processed.', reply_markup=self.button(record),
                                             disable_web_page_preview=True)
                    continue
                text = ('⚠️ This link needs an archive review; it will not be uploaded again automatically.'
                        if record['state'] == 'review' else
                        '⏳ Already queued or processing. You will receive it here when it is done.')
                await message.reply_text(text, disable_web_page_preview=True)
        await message.reply_text(f'Saved {len(records)} unique links to the database. {new} new/retry jobs queued; '
                                 f'{len(records)-new} duplicates skipped. Files go to the storage channel and you, one at a time.')
        self.wake.set()
        return records

    async def cancel(self, chat_id):
        queued = await self.ledger.cancel_queued(chat_id)
        active = self.current and self.current['chat_id'] == chat_id and self.active_task and not self.active_task.done()
        if active:
            self.active_task.cancel()
        return len(queued), bool(active)

    async def notify(self, chat_id, text, **kwargs):
        try:
            return await self.client.send_message(chat_id, text, **kwargs)
        except Exception:
            log.warning('Could not notify requesting chat')
            return None

    async def heartbeat(self, record):
        while True:
            await asyncio.sleep(30)
            await self.ledger.heartbeat(record['id'], self.owner)

    async def run(self):
        last_expire = 0
        try:
            while True:
                record = None
                try:
                    if time.monotonic()-last_expire > 60:
                        await self.ledger.expire()
                        last_expire = time.monotonic()
                    record = await self.ledger.claim(self.owner)
                    if not record:
                        self.wake.clear()
                        try:
                            await asyncio.wait_for(self.wake.wait(), timeout=30)
                        except TimeoutError:
                            pass
                        continue
                    self.lease_lost = False
                    self.current = record
                    self.active_task = asyncio.create_task(self.process(record))
                    beat = asyncio.create_task(self.heartbeat(record))
                    try:
                        done, _ = await asyncio.wait([self.active_task, beat], return_when=asyncio.FIRST_COMPLETED)
                        if beat in done:
                            # Losing database/lease access must stop work, never fall back to untracked uploads.
                            self.lease_lost = True
                            self.active_task.cancel()
                            await asyncio.gather(self.active_task, return_exceptions=True)
                            try:
                                await self.ledger.finish(record['id'], self.owner, 'review', 'Lease heartbeat failed')
                            except StorageError:
                                pass
                        else:
                            await asyncio.gather(self.active_task, return_exceptions=True)
                    finally:
                        beat.cancel()
                        await asyncio.gather(beat, return_exceptions=True)
                        if self.active_task and not self.active_task.done():
                            self.active_task.cancel()
                            await asyncio.gather(self.active_task, return_exceptions=True)
                        self.current = self.active_task = None
                except StorageError:
                    log.warning('Archive database unavailable; uploads paused')
                    await asyncio.sleep(10)
                except Exception:
                    log.error('Archive worker stopped work after an unexpected error; uploads paused')
                    await asyncio.sleep(10)
        finally:
            if self.active_task:
                self.active_task.cancel()
                await asyncio.gather(self.active_task, return_exceptions=True)

    async def process(self, record):
        key, chat = record['id'], record['chat_id']
        stored = copied = failed = skipped = 0
        final_state, error = 'failed', ''
        try:
            result = await resolve(self.http, record['url'], self.config.request_timeout)
            files = [f for f in result.files if not f.is_dir and f.dlink]
            if not files:
                raise StorageError('No downloadable files')
            for index, item in enumerate(files[:100]):
                try:
                    asset, delivered = await self.archive_file(record, result, item, index)
                    stored += 1
                    copied += int(delivered)
                except DownloadError as exc:
                    if exc.too_big:
                        skipped += 1
                    else:
                        failed += 1
                except ReviewRequired:
                    raise
                except StorageError:
                    raise  # Never continue sending if durable state is unavailable.
                except Exception:
                    failed += 1
            failed += max(0, len(files)-100)
            final_state = 'completed' if stored == len(files) else 'partial' if stored else 'failed'
        except asyncio.CancelledError:
            final_state, error = ('review' if self.lease_lost else 'cancelled'), 'Cancelled or stopped; stored files retained'
            raise
        except ReviewRequired:
            final_state, error = 'review', 'An upload may exist; operator review required before retry'
        except Exception as exc:
            error = type(exc).__name__  # Do not persist tokens, signed URLs or upstream response bodies.
        finally:
            try:
                await self.ledger.finish(key, self.owner, final_state, error)
            except StorageError:
                log.warning('Could not persist archive job completion; review may be required')
            await self.notify(chat, f'Archive job: {final_state}.\nStored: {stored} · Sent to you: {copied}'
                              f' · Failed: {failed} · Too large: {skipped}')

    async def copy_to_user(self, asset, chat):
        try:
            await self.client.copy_message(chat, asset['channel_id'], asset['message_id'])
            return True
        except FloodWait as exc:
            await asyncio.sleep(exc.value)
            try:
                await self.client.copy_message(chat, asset['channel_id'], asset['message_id'])
                return True
            except Exception:
                return False
        except Exception:
            return False

    async def archive_file(self, record, result, item, index):
        key, slot = record['id'], file_slot(item, index)
        known = await self.ledger.known_file(key, slot)
        if known:
            if known['state'] != 'stored':
                raise ReviewRequired('Interrupted upload requires review')
            return known, await self.copy_to_user(known, record['chat_id'])
        if item.size_bytes > self.config.max_bytes:
            raise DownloadError('too_big', too_big=True)
        path = None
        async with self.transfer_slot():
            try:
                await self.notify(record['chat_id'], f'Downloading: {item.name[:180]}', parse_mode=ParseMode.DISABLED)
                path = await download_to_disk(self.http, item.dlink, download_headers(result.share_url), item.name)
                # Wait for the hash thread to finish before cleanup on cancellation.
                hashing = asyncio.create_task(asyncio.to_thread(file_digest, path))
                try:
                    digest = await asyncio.shield(hashing)
                except asyncio.CancelledError:
                    await hashing
                    raise
                await self.ledger.heartbeat(key, self.owner)
                asset, fresh = await self.ledger.reserve_asset(digest, key, slot, item.name, path.stat().st_size)
                if not fresh:
                    if asset['state'] != 'stored':
                        raise ReviewRequired('Content already has an unconfirmed upload')
                else:
                    # Committed 'uploading' intent precedes Telegram. Unknown outcomes are never retried blindly.
                    caption = item.name[:900]
                    kwargs = dict(caption=caption, parse_mode=ParseMode.DISABLED, file_name=item.name)
                    ext = Path(item.name).suffix.lower()
                    try:
                        if ext in {'.mp4', '.mkv', '.webm', '.mov', '.m4v', '.avi'}:
                            sent = await self.client.send_video(self.config.storage_channel_id, video=str(path),
                                                              supports_streaming=True, **kwargs)
                            media = sent.video or sent.document
                        elif ext in {'.mp3', '.m4a', '.flac', '.wav', '.ogg', '.aac', '.opus'}:
                            sent = await self.client.send_audio(self.config.storage_channel_id, audio=str(path), **kwargs)
                            media = sent.audio or sent.document
                        else:
                            sent = await self.client.send_document(self.config.storage_channel_id, document=str(path),
                                                                 force_document=True, **kwargs)
                            media = sent.document
                    except Exception:
                        raise ReviewRequired('Telegram upload outcome is unconfirmed') from None
                    # A second upload is forbidden if this acknowledgement cannot be persisted.
                    try:
                        asset = await self.ledger.save_asset(digest, key, sent.chat.id, sent.id,
                                                             getattr(media, 'file_id', '') or '')
                    except StorageError:
                        raise ReviewRequired('Channel upload acknowledgement was not persisted') from None
                return asset, await self.copy_to_user(asset, record['chat_id'])
            finally:
                cleanup(path)
