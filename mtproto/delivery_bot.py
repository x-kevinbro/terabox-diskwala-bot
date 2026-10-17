"""Companion bot: opaque /start tokens resolve to channel messages, not raw URLs."""
from __future__ import annotations

import asyncio
import time
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.errors import FloodWait
from storage import StorageError


def create_delivery_bot(config, ledger):
    client = Client('archive-delivery-bot', api_id=config.api_id, api_hash=config.api_hash,
                    bot_token=config.delivery_bot_token, in_memory=True, workers=1,
                    parse_mode=ParseMode.DISABLED)
    gate = asyncio.Semaphore(2)
    cooldown = {}

    @client.on_message(filters.private & filters.command('start'))
    async def start(_client, message):
        user = message.from_user.id
        if not config.public_bot and config.allowed_users and user not in config.allowed_users:
            await message.reply_text('Access denied.')
            return
        now = time.monotonic()
        if now-cooldown.get(user, -100) < 10:
            await message.reply_text('Please wait a few seconds before trying again.')
            return
        if len(cooldown) > 2000:
            for key in [key for key, when in cooldown.items() if now-when > 60]:
                cooldown.pop(key, None)
            if len(cooldown) > 2000:
                await message.reply_text('The delivery bot is busy. Please try again shortly.')
                return
        cooldown[user] = now
        parts = (message.text or '').split(maxsplit=1)
        if len(parts) != 2:
            await message.reply_text('Open a Get video button from the downloader bot to retrieve an archived video.')
            return
        async with gate:
            try:
                record, assets = await ledger.retrieval(parts[1].strip())
                if not record:
                    await message.reply_text('That video link is invalid or no longer available.')
                    return
                if not assets:
                    await message.reply_text('This link has no stored videos yet. It may still be processing or need a retry/review.')
                    return
                sent = failed = 0
                for asset in assets[:100]:
                    try:
                        await client.copy_message(message.chat.id, asset['channel_id'], asset['message_id'])
                        sent += 1
                    except FloodWait as exc:
                        await asyncio.sleep(exc.value)
                        try:
                            await client.copy_message(message.chat.id, asset['channel_id'], asset['message_id'])
                            sent += 1
                        except Exception:
                            failed += 1
                    except Exception:
                        failed += 1
                await message.reply_text(f'Sent: {sent}. Unavailable: {failed}. Archive status: {record["state"]}.')
            except StorageError:
                await message.reply_text('Archive lookup is temporarily unavailable. Please try again later.')
    return client
