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

## Graph / Telegraph automatic batches

Send a public `https://graph.org/...` or `https://telegra.ph/...` article in a
private chat. The bot scans article hyperlinks and plain-text URLs, removes
duplicates, and automatically resolves, downloads, and uploads supported
Diskwala/Terabox files **one at a time, in article order**. Individual failures
are counted and processing continues. It does not crawl linked articles.

- Up to 5 articles, 50 unique share links, and 100 attempted files per batch.
- Up to 4 pending batches globally; one batch per chat. Batches run sequentially.
- `/cancel` stops your batch and removes temporary files. Sent files remain.
- Files above `MAX_FILE_MB` are skipped with a direct link instead.
- Batches are in memory and do not resume after a restart.
- Ordinary direct share links keep their existing manual download buttons.
- Health includes build identifier, active transfers, queued transfers, and
  pending batches. Tokens and media are not exposed in the health response.

## Offline regression tests

From the repository root, after installing `mtproto/requirements.txt`:

```bash
python -m unittest discover -s mtproto/tests -v
```

Tests use fake credentials and mocked Telegram/resolver/download calls; they
never send a message or download media. Coverage includes article parsing,
redirect restrictions, limits, sequential delivery, cancellation, cleanup,
transfer counters, and existing direct-link behavior.

## Optional Turso archive and companion delivery bot

**Opt-in and not needed for the existing downloader:** keep `ARCHIVE_ENABLED=false`
for the current manual direct-link / automatic Graph-batch behavior. The archive
feature must not be enabled until every credential and channel permission below
is configured. There is no local-file database fallback.

### Setup before deployment

1. Create a **Turso Cloud database**. Obtain its database URL and a **database-scoped
   read/write auth token**. Do not use an organization/platform management token.
   The bot uses the official authenticated `POST /v2/pipeline` SQL protocol with
   bound parameters; no local sync copy or additional native database dependency.
2. Create a **second Telegram bot** using BotFather. Set its token as a secret;
   the username is discovered at runtime, not hard-coded. Both bots run as separate
   MTProto clients inside this service using the existing API_ID/API_HASH.
3. Create a private storage channel, add **both bots as admins with posting
   permission**, and obtain its numeric channel ID (`-100...`). Use a channel that
   allows message copying; protected-content messages cannot be redistributed.
4. Set `TURSO_DATABASE_URL`, `TURSO_AUTH_TOKEN`, `STORAGE_CHANNEL_ID`, and
   `DELIVERY_BOT_TOKEN` in the host's private environment/secret controls. Never
   put real values in this repository, screenshots, chat, or tool arguments.
5. Enable `ARCHIVE_ENABLED=true`, then deploy the `mtproto/` Docker build. Startup
   initializes the schema and checks both bot identities and channel permissions.
   Health becomes ready only after the companion and archive worker are running.

Official protocol: https://docs.turso.tech/sdk/http/reference

### Behavior in archive mode

- Direct supported TeraBox/Diskwala links and Graph/Telegraph-extracted links are
  saved in one atomic insert per submitted batch **before any new downloads**.
- Supported mirror `/s/...` URLs and known `surl` forms share a canonical key.
  Tracking parameters/fragments are removed; password and unknown query options
  are preserved conservatively. Unknown/protected/expired links can still fail.
- A unique link key and conditional database claims prevent concurrent duplicate
  jobs. A global processing lease keeps archive jobs sequential across workers.
- New files are streamed to disk, hashed with SHA-256, uploaded to the storage
  channel, durably recorded, then copied to the requesting user. Files are cleaned
  up on success, error, and cancellation. Existing file-size/disk checks apply.
- Identical downloaded bytes reuse one channel message even across different
  share links. Different URLs may still require downloading once to calculate
  the hash; this is not perceptual/video-similarity deduplication.
- Completed duplicate links get **Already processed → Get video**, which opens
  `https://t.me/<delivery-bot>?start=<opaque-token>`. They do not create another
  storage post or get media resent by the downloader bot.
- Pending duplicates are not requeued. Failed/partial/cancelled jobs can be retried
  by resending the link; confirmed stored files are reused. Ordinary download
  buttons from before archive mode are rejected to prevent bypassing the ledger.
- The companion bot copies messages from the channel, rather than using one bot's
  file ID in another bot. It handles multiple files, pending/invalid records, and
  retrieval cooldowns. Retrieval tokens are unguessable **bearer links**: anyone
  with a valid link can retrieve media when PUBLIC_BOT=true. Treat them accordingly.
- `/cancel` cancels that chat's queued archive jobs and its active transfer, plus
  any active page scan. Other chats' jobs and stored channel messages are retained.
- Unclaimed queued jobs survive process restarts. Processing leases are refreshed
  every 30 seconds; expired processing jobs require review, not automatic reupload.
- Up to 5 pages / 50 links per submission, up to 100 attempted files per share.
  The database stores source URL, first source page, requesting chat ID, statuses,
  opaque retrieval token, content hashes, and channel/file/message identifiers.
  Signed CDN URLs and bot/database tokens are not stored in the ledger.

### Crash safety / operator review

Telegram uploads and database commits are not one distributed transaction.
The ledger commits an `uploading` intent before contacting Telegram. If the
response is lost, the bot is cancelled during an upload, or confirmation cannot
be persisted, it refuses to upload those bytes again automatically. It marks the
job for review or detects the unconfirmed asset on the next retry. **No exact-once
promise is made across those two systems.**

A trusted operator must inspect the private channel and the relevant
`archive_links`, `archive_files`, and `archive_assets` records. If a message exists,
verify the file and reconcile the asset with its actual numeric channel/message
IDs and `stored` state. Only clear an upload intent for retry after confirming
that no channel post exists. There is intentionally no public reset/delete button
that could bypass this safeguard. Database outages pause work instead of sending
untracked media.

There is no automatic backfill of videos posted before this feature was enabled,
no guarantee of detecting altered/re-encoded copies of the same video, and no
assumption that a share's contents can change without affecting its identity.
The archive is not a backup of Telegram itself: deleted/inaccessible channel
messages cannot be recovered from database metadata alone.

### Validation and rollout gate

Offline tests use SQLite to exercise the same SQL and mock all Telegram, HTTP,
resolver, and media-transfer boundaries. They cover both current and opt-in modes,
identity normalization, queue claims, duplicate content, restart/cancellation,
unknown uploads, failure to persist confirmation, channel-first copying, and
second-bot retrieval. They do not prove connectivity to a real Turso database or
end-to-end Telegram delivery.

Before enabling in production: configure secrets privately, verify real Turso
access and both bot permissions, test one small authorized file, resubmit its link
and a Graph page containing it, follow the retrieval button, and confirm exactly
one storage post. Also test blocked-user delivery and a restart. Keep the previous
Koyeb archive/configuration for rollback. Running two MTProto clients adds overhead;
verify the service's memory usage under an actual upload before opening public use.
