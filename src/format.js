import { config } from './config.js';
import { fileEmoji } from './utils.js';

export const esc = (s) =>
  String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

const MAX_BUTTONS = 10; // Telegram allows up to 100, but keep messages tidy.

const PROVIDER_STYLE = {
  diskwala: { icon: '🎬', label: 'DISKWALA' },
  terabox: { icon: '🌐', label: 'TERABOX' },
  youtube: { icon: '📺', label: 'YOUTUBE' },
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
    `${style.icon} <b><u>${style.label} — LINK READY</u></b>`,
    '',
    files.length
      ? `<code>▎✅ ${files.length} file${files.length === 1 ? '' : 's'} resolved</code>`
      : '<code>▎📭 Empty share</code>',
    '',
    DIVIDER,
  ];

  shown.forEach((f, i) => {
    const tooBig = f.size_bytes > maxBytes;
    lines.push(`${fileEmoji(f.name)} ${esc(f.name)}`);
    lines.push(
      `💾 ${esc(f.size)}` + (tooBig ? '  •  ⚠️ <i>over chat limit — link only</i>' : ''),
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
    '<b>👇 Pick an action:</b>',
    '',
    '<blockquote>⬇️ <b>Download</b> — file lands right here in chat\n🔗 <b>Direct link</b> — raw URL, tap to copy</blockquote>',
  );

  const keyboard = shown.map((f, i) => [
    {
      text: files.length > 1 ? `⬇️ Download #${i + 1}` : '⬇️ Download',
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
