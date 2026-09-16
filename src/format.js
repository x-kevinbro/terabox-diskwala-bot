import { config } from './config.js';
import { fileEmoji } from './utils.js';

export const esc = (s) =>
  String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

const trunc = (s, n = 22) => (s.length > n ? `${s.slice(0, n - 1)}…` : s);

const MAX_BUTTONS = 10; // Telegram allows up to 100, but keep messages tidy.

const PROVIDER_STYLE = {
  diskwala: { icon: '🎬', label: 'DISKWALA' },
  terabox: { icon: '🌐', label: 'TERABOX' },
};

const DIVIDER = '┄┄┄┄┄┄┄┄┄┄┄┄┄┄';

// Build the info message + inline keyboard for a resolved share link.
// Button callback_data format: "dl:<cacheId>:<fileIndex>" / "ln:<cacheId>:<fileIndex>"
// (Telegram caps callback_data at 64 bytes — hence the short cache id.)
export function buildInfoPayload(result, cacheId) {
  const files = result.files.filter((f) => !f.is_dir);
  const shown = files.slice(0, MAX_BUTTONS);
  const maxBytes = config.maxFileMb * 1024 * 1024;
  const style = PROVIDER_STYLE[result.provider] || {
    icon: '📦',
    label: result.provider.toUpperCase(),
  };

  const lines = [
    `${style.icon} <b>${style.label} — LINK READY</b>`,
    files.length
      ? `<blockquote>✅ ${files.length} file${files.length === 1 ? '' : 's'} resolved</blockquote>`
      : '<blockquote>📭 Empty share</blockquote>',
    DIVIDER,
  ];

  shown.forEach((f, i) => {
    const tooBig = f.size_bytes > maxBytes;
    lines.push(`${fileEmoji(f.name)} <b>${esc(f.name)}</b>`);
    lines.push(
      `   💾 <code>${esc(f.size)}</code>` +
        (tooBig ? '  •  ⚠️ <i>over chat limit — link only</i>' : ''),
    );
    if (i < shown.length - 1) lines.push('');
  });

  if (files.length > shown.length) {
    lines.push(
      '',
      `➕ <i>${files.length - shown.length} more file(s) — only the first ${MAX_BUTTONS} get buttons.</i>`,
    );
  }
  if (!files.length) {
    lines.push('Only folders found in this share — nothing to download directly.');
  }

  lines.push(
    DIVIDER,
    '',
    '👇 <b>Pick an action:</b>',
    '⬇️ <b>Download</b> — file lands right here in chat',
    '🔗 <b>Direct link</b> — raw URL, tap to copy',
  );

  const keyboard = shown.map((f, i) => [
    {
      text: `⬇️ Download${files.length > 1 ? ` #${i + 1}` : ''} (${f.size})`,
      callback_data: `dl:${cacheId}:${i}`,
    },
    {
      text: files.length > 1 ? `🔗 #${i + 1}` : '🔗 Direct Link',
      callback_data: `ln:${cacheId}:${i}`,
    },
  ]);

  return {
    text: lines.join('\n'),
    reply_markup: keyboard.length ? { inline_keyboard: keyboard } : undefined,
    fileCount: files.length,
  };
}
