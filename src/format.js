import { config } from './config.js';

export const esc = (s) =>
  String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

const trunc = (s, n = 30) => (s.length > n ? `${s.slice(0, n - 1)}…` : s);

const MAX_BUTTONS = 10; // Telegram allows up to 100, but keep messages tidy.

// Build the info message + inline keyboard for a resolved share link.
// Button callback_data format: "dl:<cacheId>:<fileIndex>" / "ln:<cacheId>:<fileIndex>"
// (Telegram caps callback_data at 64 bytes — hence the short cache id.)
export function buildInfoPayload(result, cacheId) {
  const files = result.files.filter((f) => !f.is_dir);
  const shown = files.slice(0, MAX_BUTTONS);
  const maxBytes = config.maxFileMb * 1024 * 1024;

  const lines = [`📦 <b>${esc(result.provider.toUpperCase())} link detected</b>`, ''];
  shown.forEach((f, i) => {
    const tooBig = f.size_bytes > maxBytes ? '  ⚠️ <i>over TG limit — link only</i>' : '';
    lines.push(`${i + 1}. <b>${esc(f.name)}</b>`);
    lines.push(`    💾 ${esc(f.size)}${tooBig}`);
  });
  if (files.length > shown.length) {
    lines.push(`…and ${files.length - shown.length} more file(s) — only the first ${MAX_BUTTONS} get buttons.`);
  }
  if (!files.length) {
    lines.push('Only folders found in this share — nothing to download directly.');
  }
  lines.push('', '⬇️ = receive the file here   •   🔗 = get the direct link');

  const keyboard = shown.map((f, i) => [
    { text: `⬇️ ${trunc(f.name)} (${f.size})`, callback_data: `dl:${cacheId}:${i}` },
    { text: '🔗', callback_data: `ln:${cacheId}:${i}` },
  ]);

  return {
    text: lines.join('\n'),
    reply_markup: keyboard.length ? { inline_keyboard: keyboard } : undefined,
    fileCount: files.length,
  };
}
