# Diskwala + Terabox Downloader (MTProto build)

Telegram bot that resolves Diskwala and Terabox share links and delivers the
file in chat. It speaks **MTProto** (Pyrogram API via `kurigram`), so uploads
go up to **2000 MB** instead of the 50 MB HTTP Bot API limit.

## Where to put API_ID, API_HASH and BOT_TOKEN

They are **environment variables** - never hard-code them.

| Variable | Where to get it |
|---|---|
| `API_ID` | https://my.telegram.org -> API development tools |
| `API_HASH` | same page as `API_ID` |
| `BOT_TOKEN` | @BotFather -> your bot -> API token |

**On Koyeb:** Service -> Settings -> Environment variables -> add the three
variables (mark `API_HASH` and `BOT_TOKEN` as secrets), then redeploy.

**Locally:** copy `.env.example` to `.env` and fill in the top three values.
`config.py` loads `.env` automatically; real environment variables win.

## Memory safety on a 512 MB instance

- Downloads stream to disk in 1 MiB chunks (`downloader.py`); the file is never
  held in RAM.
- Uploads stream from disk in small chunks by the MTProto client.
- `MAX_CONCURRENT_JOBS=1` - a single transfer at a time.
- Temp files are deleted in a `finally` block, so disk is freed even on error.
- Free disk is checked before staging a file.

Typical resident memory stays around 60-90 MB regardless of file size.

## Run locally

```bash
pip install -r requirements.txt
cp .env.example .env   # then edit it
python bot.py
```

## Health endpoint

`GET /api/health` returns JSON with bot username, active jobs and the upload
ceiling - use it for Koyeb health checks and UptimeRobot.
