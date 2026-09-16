import fs from 'node:fs';
import path from 'node:path';
import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { config } from './src/config.js';
import { cookiePool } from './src/cookies.js';
import * as tg from './src/telegram.js';
import { extractMediaLinks } from './src/detect.js';
import { buildInfoPayload, esc } from './src/format.js';
import { formatSize } from './src/utils.js';
import { Cache } from './src/cache.js';

if (!config.botToken) {
  console.error('BOT_TOKEN is missing — set it in .env and restart.');
  process.exit(1);
}

const downloadDir = path.resolve(config.downloadDir);
fs.mkdirSync(downloadDir, { recursive: true });

const cache = new Cache();
const maxBytes = config.maxFileMb * 1024 * 1024;

// --- Access control (private bot) ---
// Owner ids come from ALLOWED_USERS (comma-separated) and/or data/owner.json.
// When neither exists, the first /start claims ownership (persisted to disk).
const ownerFile = path.resolve('data/owner.json');
const ownerIds = new Set(
  (process.env.ALLOWED_USERS || '').split(',').map((s) => s.trim()).filter(Boolean),
);
try {
  for (const id of JSON.parse(fs.readFileSync(ownerFile, 'utf8'))) ownerIds.add(String(id));
} catch {}
function claimOwner(chatId) {
  ownerIds.add(String(chatId));
  fs.mkdirSync(path.dirname(ownerFile), { recursive: true });
  fs.writeFileSync(ownerFile, JSON.stringify([...ownerIds]));
  console.log(`[access] owner claimed: ${chatId}`);
}

const VIDEO_EXT = new Set(['mp4', 'mkv', 'webm', 'mov', 'm4v', 'avi', 'mpg', 'mpeg']);
const AUDIO_EXT = new Set(['mp3', 'm4a', 'flac', 'wav', 'ogg', 'aac', 'opus', 'wma']);
const extOf = (name) => {
  const m = /\.([A-Za-z0-9]{1,5})$/.exec(String(name || '').trim());
  return m ? m[1].toLowerCase() : '';
};
const mediaKind = (name) => {
  const e = extOf(name);
  if (VIDEO_EXT.has(e)) return 'video';
  if (AUDIO_EXT.has(e)) return 'audio';
  return 'document';
};

// Build a clean Telegram filename that ALWAYS keeps its extension. Telegram
// clients can't open files whose names get truncated past the extension.
function safeFileName(name, dlink) {
  let base = String(name || '').trim().replace(/[\\/:*?"<>|\r\n]+/g, '_');
  if (/^https?:/i.test(base)) base = '';
  let ext = extOf(base);
  if (!ext && dlink) {
    try {
      ext = extOf(decodeURIComponent(new URL(dlink).pathname));
    } catch {
      ext = '';
    }
  }
  let core = base;
  if (ext && core.toLowerCase().endsWith('.' + ext)) core = core.slice(0, -(ext.length + 1));
  core = core.replace(/\.+$/, '').trim() || 'file';
  if (core.length > 60) core = core.slice(0, 60).trimEnd();
  return ext ? `${core}.${ext}` : core;
}

// Download a dlink to a temp file, enforcing the size cap both from the
// content-length header and while streaming.
async function downloadToDisk(dlink, headers, ext = '', onProgress = null) {
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
  const tmp = path.join(
    downloadDir,
    `dl_${Date.now()}_${Math.random().toString(36).slice(2, 8)}${ext ? `.${ext}` : ''}`,
  );
  let received = 0;
  const guard = new Transform({
    transform(chunk, enc, cb) {
      received += chunk.length;
      if (onProgress) {
        try {
          onProgress(received, len);
        } catch {}
      }
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

  // Private bot: first /start claims ownership; everyone else is rejected.
  if (!ownerIds.size) {
    if (!text.startsWith('/start')) {
      await tg.sendMessage(chatId, "🔒 This bot isn't claimed yet. Send /start to become its owner.");
      return;
    }
    claimOwner(chatId);
    await tg.sendMessage(
      chatId,
      `✅ <b>You are now the owner</b> — only your account can use this bot. (ID: <code>${chatId}</code>)`,
    );
  } else if (!ownerIds.has(String(chatId))) {
    console.log(`[access] blocked unauthorized chat ${chatId}`);
    await tg.sendMessage(chatId, '🔒 Private bot — access denied.');
    return;
  }

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
  if (ownerIds.size && !ownerIds.has(String(chatId))) {
    await tg.answerCallbackQuery(cq.id, '🔒 Private bot', true);
    return;
  }
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
  let thumbPath = null;
  try {
    const fname = safeFileName(file.name, file.dlink);
    const kind = mediaKind(fname);
    const headers = entry.provider.downloadHeaders(cookiePool.next() || '');
    let lastEdit = 0;
    const onProgress = (received, total) => {
      const now = Date.now();
      if (now - lastEdit < 4000) return; // stay under Telegram's edit rate limit
      lastEdit = now;
      const pct = total ? Math.floor((received / total) * 100) : null;
      const bar =
        pct == null
          ? ''
          : `\n${'▓'.repeat(Math.floor(pct / 10))}${'░'.repeat(10 - Math.floor(pct / 10))} ${pct}%`;
      tg.editMessageText(
        chatId,
        status.message_id,
        `⏳ Downloading <b>${esc(fname)}</b>… ${esc(formatSize(received))}${total ? ` / ${esc(formatSize(total))}` : ''}${bar}`,
      );
    };
    tmp = await downloadToDisk(file.dlink, headers, extOf(fname), onProgress);

    // Attach the provider thumbnail to playable media when available.
    if (file.thumbnail && (kind === 'video' || kind === 'audio')) {
      try {
        const t = await fetch(file.thumbnail, { signal: AbortSignal.timeout(15_000) });
        if (t.ok) {
          thumbPath = `${tmp}.jpg`;
          fs.writeFileSync(thumbPath, Buffer.from(await t.arrayBuffer()));
        }
      } catch {}
    }

    await tg.editMessageText(
      chatId,
      status.message_id,
      `📤 Uploading <b>${esc(fname)}</b> to Telegram… (big files can take a while)`,
    );
    await tg.sendChatAction(
      chatId,
      kind === 'video' ? 'upload_video' : kind === 'audio' ? 'upload_audio' : 'upload_document',
    );
    const caption = `📦 ${esc(fname)} (${esc(file.size)})`;
    try {
      await tg.sendFile({ chatId, filePath: tmp, filename: fname, caption, kind, thumbPath });
    } catch (e1) {
      if (kind === 'document') throw e1;
      // Telegram may reject some containers as video/audio — retry as a plain document.
      console.error(`${kind} upload failed (${e1.message}) — retrying as document`);
      await tg.sendFile({ chatId, filePath: tmp, filename: fname, caption, kind: 'document' });
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
    if (thumbPath) fs.unlink(thumbPath, () => {});
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
