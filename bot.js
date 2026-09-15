import fs from 'node:fs';
import path from 'node:path';
import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { config } from './src/config.js';
import { cookiePool } from './src/cookies.js';
import * as tg from './src/telegram.js';
import { extractMediaLinks } from './src/detect.js';
import { buildInfoPayload, esc } from './src/format.js';
import { Cache } from './src/cache.js';

if (!config.botToken) {
  console.error('BOT_TOKEN is missing — set it in .env and restart.');
  process.exit(1);
}

const downloadDir = path.resolve(config.downloadDir);
fs.mkdirSync(downloadDir, { recursive: true });

const cache = new Cache();
const maxBytes = config.maxFileMb * 1024 * 1024;

const VIDEO_EXT = new Set(['mp4', 'mkv', 'webm', 'mov', 'm4v', 'avi', 'mpg', 'mpeg']);
const isVideo = (name) => VIDEO_EXT.has(String(name).split('.').pop().toLowerCase());

// Download a dlink to a temp file, enforcing the size cap both from the
// content-length header and while streaming.
async function downloadToDisk(dlink, headers) {
  const resp = await fetch(dlink, { headers, redirect: 'follow' });
  if (!resp.ok && resp.status !== 206) {
    resp.body?.cancel().catch(() => {});
    throw new Error(`download failed (HTTP ${resp.status})`);
  }
  const len = Number(resp.headers.get('content-length') || 0);
  if (len > maxBytes) {
    resp.body?.cancel().catch(() => {});
    const e = new Error('too_big');
    e.tooBig = true;
    throw e;
  }
  const tmp = path.join(downloadDir, `dl_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`);
  let received = 0;
  const guard = new Transform({
    transform(chunk, enc, cb) {
      received += chunk.length;
      if (received > maxBytes) {
        const e = new Error('too_big');
        e.tooBig = true;
        cb(e);
        return;
      }
      cb(null, chunk);
    },
  });
  try {
    await pipeline(Readable.fromWeb(resp.body), guard, fs.createWriteStream(tmp));
  } catch (err) {
    fs.unlink(tmp, () => {});
    throw err;
  }
  return tmp;
}

async function handleMessage(msg) {
  const chatId = msg.chat && msg.chat.id;
  if (!chatId) return;
  const text = msg.text || msg.caption || '';

  if (text.startsWith('/start') || text.startsWith('/help')) {
    await tg.sendMessage(
      chatId,
      `👋 <b>Diskwala & Terabox Downloader Bot</b>\n\n` +
        `Send me a Diskwala or Terabox share link and I'll reply with the file info and download buttons.\n\n` +
        `• Files up to ${config.maxFileMb} MB land right here in the chat\n` +
        `• Bigger files get a direct download link instead\n\n` +
        `Just paste a link 🔗`,
    );
    return;
  }

  const links = extractMediaLinks(text);
  if (!links.length) {
    if (text.startsWith('/')) return; // ignore unknown commands silently
    await tg.sendMessage(
      chatId,
      '🤔 No Diskwala/Terabox link found there. Send a share link like <code>https://terabox.com/s/xxxx</code>',
    );
    return;
  }

  for (const { url, provider } of links) {
    const status = await tg.sendMessage(chatId, '🔍 Resolving link…');
    try {
      const result = await provider.resolveInfo(url, {
        cookies: cookiePool,
        timeoutMs: config.requestTimeoutMs,
      });
      const id = cache.put({ result, provider });
      const payload = buildInfoPayload(result, id);
      await tg.deleteMessage(chatId, status.message_id);
      await tg.sendMessage(chatId, payload.text, {
        reply_markup: payload.reply_markup,
        reply_to_message_id: msg.message_id,
      });
    } catch (err) {
      await tg.editMessageText(chatId, status.message_id, `❌ <b>Failed:</b> ${esc(err.message)}`);
    }
  }
}

async function handleCallback(cq) {
  const chatId = cq.message && cq.message.chat && cq.message.chat.id;
  const [action, id, idxStr] = String(cq.data || '').split(':');
  const entry = cache.get(id);
  if (!chatId || !entry || !['dl', 'ln'].includes(action)) {
    await tg.answerCallbackQuery(cq.id, '⚠️ This button expired — send the link again', true);
    return;
  }
  const files = entry.result.files.filter((f) => !f.is_dir);
  const file = files[Number.parseInt(idxStr, 10)];
  if (!file || !file.dlink) {
    await tg.answerCallbackQuery(cq.id, 'File not found', true);
    return;
  }

  // "🔗" button — just send the direct link.
  if (action === 'ln') {
    await tg.answerCallbackQuery(cq.id, '🔗 Direct link sent below');
    await tg.sendMessage(
      chatId,
      `🔗 <b>${esc(file.name)}</b> (${esc(file.size)})\n${file.dlink}\n\n<i>Direct links expire — use it soon.</i>`,
    );
    return;
  }

  // "⬇️" button — deliver the file into the chat.
  if (file.size_bytes > maxBytes) {
    await tg.answerCallbackQuery(
      cq.id,
      `⚠️ ${file.size} is over the ${config.maxFileMb} MB bot limit — link only`,
      true,
    );
    await tg.sendMessage(
      chatId,
      `⚠️ <b>${esc(file.name)}</b> (${esc(file.size)}) is larger than Telegram's ${config.maxFileMb} MB bot upload limit, so I can't send it here.\n\n🔗 Direct link:\n${file.dlink}\n\n<i>Tip: run a local Bot API server to raise the limit to 2 GB — see README.</i>`,
    );
    return;
  }

  await tg.answerCallbackQuery(cq.id, '⏳ Working on it…');
  const status = await tg.sendMessage(
    chatId,
    `⏳ Downloading <b>${esc(file.name)}</b> (${esc(file.size)})…`,
  );
  let tmp = null;
  try {
    const headers = entry.provider.downloadHeaders(cookiePool.next() || '');
    tmp = await downloadToDisk(file.dlink, headers);
    await tg.editMessageText(
      chatId,
      status.message_id,
      `📤 Uploading <b>${esc(file.name)}</b> to Telegram…`,
    );
    const video = isVideo(file.name);
    await tg.sendChatAction(chatId, video ? 'upload_video' : 'upload_document');
    const caption = `📦 ${esc(file.name)} (${esc(file.size)})`;
    try {
      await tg.sendFile({ chatId, filePath: tmp, filename: file.name, caption, asVideo: video });
    } catch (e1) {
      if (!video) throw e1;
      // Some containers (e.g. mkv) are rejected by sendVideo — fall back to document.
      await tg.sendFile({ chatId, filePath: tmp, filename: file.name, caption, asVideo: false });
    }
    await tg.deleteMessage(chatId, status.message_id);
  } catch (err) {
    const reason = err.tooBig
      ? `the file exceeded the ${config.maxFileMb} MB limit while downloading`
      : esc(err.message);
    await tg.editMessageText(
      chatId,
      status.message_id,
      `❌ <b>${esc(file.name)}</b> failed: ${reason}\n\n🔗 Direct link:\n${file.dlink}`,
    );
  } finally {
    if (tmp) fs.unlink(tmp, () => {});
  }
}

async function main() {
  try {
    const me = await tg.getMe();
    console.log(`Logged in as @${me.username} (id ${me.id})`);
  } catch (err) {
    console.warn(`getMe failed (${err.message}) — will keep retrying via the poll loop.`);
  }
  console.log(`Max file size: ${config.maxFileMb} MB | API root: ${config.apiRoot}`);
  console.log(`Terabox cookies configured: ${cookiePool.size}`);
  console.log('Listening for messages…');

  let offset = 0;
  let failures = 0;
  for (;;) {
    try {
      const updates = await tg.getUpdates(offset);
      failures = 0;
      for (const u of updates) {
        offset = u.update_id + 1;
        if (u.message) {
          handleMessage(u.message).catch((e) => console.error('message handler error:', e.message));
        } else if (u.callback_query) {
          handleCallback(u.callback_query).catch((e) =>
            console.error('callback handler error:', e.message),
          );
        }
      }
    } catch (err) {
      failures += 1;
      const waitMs = Math.min(15_000, 1_000 * failures);
      console.error(`Polling error: ${err.message} — retrying in ${waitMs / 1000}s`);
      await new Promise((r) => setTimeout(r, waitMs));
    }
  }
}

main();
