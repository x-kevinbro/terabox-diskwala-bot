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

// Quality-picker card (YouTube): the actual download links resolve lazily
// when a quality button is tapped. Callback data: q:<cacheId>:<format>:<type>
// for downloads, k:<cacheId>:<format>:<type> for the direct link.
function buildQualityPayload(result, cacheId) {
  const style = PROVIDER_STYLE[result.provider] || {
    icon: '📦',
    label: result.provider.toUpperCase(),
  };
  const lines = [
    `${style.icon} <b><u>${style.label} — LINK READY</u></b>`,
    '',
    '<code>▎✅ Video found</code>',
    '',
    DIVIDER,
    `🎬 ${esc(result.title || 'YouTube video')}`,
  ];
  if (result.author) lines.push(`📺 ${esc(result.author)}`);
  lines.push(
    DIVIDER,
    '',
    '<b>👇 Pick a quality:</b>',
    '',
    '<blockquote>📺 <b>MP4</b> — video (360p / 720p)\n🎵 <b>MP3</b> — audio only</blockquote>',
  );

  const keyboard = [
    [
      { text: '📺 360p MP4', callback_data: `q:${cacheId}:360:mp4` },
      { text: '📺 720p MP4', callback_data: `q:${cacheId}:720:mp4` },
    ],
    [
      { text: '🎵 MP3 Audio', callback_data: `q:${cacheId}:best:mp3` },
      { text: '🔗 360p link', callback_data: `k:${cacheId}:360:mp4` },
    ],
  ];

  return {
    text: lines.join('\n'),
    reply_markup: { inline_keyboard: keyboard },
    fileCount: 1,
  };
}

// Build the info message + inline keyboard for a resolved share link.
// Button callback_data format: "dl:<cacheId>:<fileIndex>" / "ln:<cacheId>:<fileIndex>"
// (Telegram caps callback_data at 64 bytes — hence the short cache id.)
export function buildInfoPayload(result, cacheId) {
  if (Array.isArray(result.qualities) && result.qualities.length) {
    return buildQualityPayload(result, cacheId);
  }

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
