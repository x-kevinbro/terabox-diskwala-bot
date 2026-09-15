# Diskwala & Terabox Telegram Bot

Send this bot a **Diskwala** or **Terabox** share link and it replies with the file
info and inline **Download** buttons. Tap a button and the file is delivered straight
into the chat. Tap **🔗** to get the raw direct link instead.

```
You:  https://terabox.com/s/1abc…
Bot:  📦 TERABOX link detected
      1. movie.mp4
         💾 734.00 MB
      ⬇️ = receive the file here • 🔗 = get the direct link
      [ ⬇️ movie.mp4 (734.00 MB) ] [ 🔗 ]
```

Zero npm dependencies — plain Node.js ≥ 18.17, nothing to install. Long polling, so
no public URL or webhook is required.

## Deploy to Heroku (one click)

[![Deploy](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/x-kevinbro/terabox-diskwala-bot)

Or with the CLI:

```bash
heroku create --region us
heroku config:set BOT_TOKEN=xxx TERABOX_COOKIES="ndus=xxx; lang=en" MAX_FILE_MB=48
git push heroku main
heroku scale worker=1:eco
heroku logs --tail
```

> **Note on 2GB files:** Heroku dynos can't stage multi-GB files (small ephemeral
> disk / RAM) and the cloud Bot API caps bot uploads at 50MB regardless. For true
> 2GB delivery, run the bot on a VPS together with a local Bot API server — see
> below. On Heroku, set `MAX_FILE_MB=48`.

## Run anywhere (VPS / your PC)

1. **Bot token** — from @BotFather.
2. **Terabox cookie (required for Terabox links)** — log in at
   [terabox.com](https://www.terabox.com), DevTools (`F12`) → **Application** →
   **Cookies** → `https://www.terabox.com` → copy `ndus=...` into `.env`:
   ```
   TERABOX_COOKIES=ndus=XXXX; browserid=YYYY; lang=en
   ```
   Use a throwaway account, and never commit `.env`.
3. **Run:**
   ```bash
   cp .env.example .env   # fill in values
   node bot.js            # or: pm2 start bot.js --name tera-bot
   ```

## Using the bot

| Action | Result |
| --- | --- |
| Send a share link | Bot resolves it and shows every file with size + buttons |
| Tap **⬇️ download** | Bot downloads the file and uploads it into the chat (with progress messages) |
| Tap **🔗** | Bot sends the direct download link as a message |
| `/start` or `/help` | Help text |

Folder shares: one button per file (first 10 get buttons). Video files (mp4/mkv/…)
are sent as Telegram videos when possible, otherwise as documents.

## ⚠️ File size limit (important)

The official Telegram Bot API lets bots upload files **up to 50 MB**. The default cap
is `MAX_FILE_MB=48` — bigger files automatically get a direct link instead.

To raise the limit to **2 GB**, run a local Bot API server
([`telegram-bot-api`](https://core.telegram.org/bots/api#using-a-local-bot-api-server))
on the same machine and set in `.env`:

```
TELEGRAM_API_ROOT=http://localhost:8081
MAX_FILE_MB=2000
```

Quick start for the local API server (needs `api_id`/`api_hash` from
[my.telegram.org](https://my.telegram.org) → API development tools):

```bash
docker run -d --name tg-api --restart always \
  -p 8081:8081 \
  -v tg-bot-api-data:/var/lib/telegram-bot-api \
  aiogram/telegram-bot-api:latest \
  --api-id=YOUR_API_ID --api-hash=YOUR_API_HASH --local
```

## Configuration

| Env var | Default | Description |
| --- | --- | --- |
| `BOT_TOKEN` | — | Telegram bot token from @BotFather (required) |
| `TERABOX_COOKIES` | — | Terabox cookie(s), comma-separated (required for Terabox) |
| `MAX_FILE_MB` | `48` | Max upload size into Telegram |
| `TELEGRAM_API_ROOT` | `https://api.telegram.org` | Override for a local Bot API server |
| `REQUEST_TIMEOUT_MS` | `25000` | Upstream resolver timeout |
| `DOWNLOAD_DIR` | `./downloads` | Temp dir for files in transit (auto-cleaned) |
| `EXTRA_HOSTS` | — | Extra Terabox-family mirror domains to accept |

## 🔐 Security

- Never commit `.env`; `.gitignore` already excludes it. Set secrets as Heroku
  config vars, not in the repo.
- If your bot token or `ndus` cookie ever leak, revoke them immediately
  (@BotFather → `/revoke`; log out of terabox.com).

## Notes & limitations

- Unofficial tooling. Terabox can change their internal API at any time; the most
  common failure is an expired cookie (`Failed to extract tokens` → re-login, refresh
  `TERABOX_COOKIES`, restart).
- The **Diskwala resolver is experimental** — it scans the share page for direct media
  URLs and may need pattern updates if their layout changes. Terabox-family links are
  the stable path.
- Download buttons expire after 10 minutes (Telegram limits callback data size, so
  results are cached in memory). Just re-send the link.
- Only use this for content you have the right to download, and respect the terms of
  service of Telegram, Terabox, and Diskwala.
