import { formatSize } from '../utils.js';

export const name = 'youtube';

export const hosts = new Set([
  'youtube.com',
  'www.youtube.com',
  'm.youtube.com',
  'music.youtube.com',
  'youtu.be',
  'www.youtu.be',
  'youtube-nocookie.com',
  'www.youtube-nocookie.com',
]);

const UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36';

// YouTube downloads go through the same hosted resolver account as Terabox.
// The upstream can take ~45s to convert, so quality links resolve lazily.
const YT_API_BASE =
  process.env.YTDL_API_BASE || process.env.TERABOX_API_BASE || 'https://api-dark-shan-yt.koyeb.app';
const YT_API_KEY = process.env.YTDL_API_KEY || process.env.TERABOX_API_KEY || '';
const YT_MP3_FORMAT = process.env.YTDL_MP3_FORMAT || '320';

export const QUALITIES = [
  { id: '360', type: 'mp4', label: '360p MP4' },
  { id: '720', type: 'mp4', label: '720p MP4' },
  { id: 'best', type: 'mp3', label: 'MP3 Audio' },
];

export function downloadHeaders() {
  return { 'User-Agent': UA, Accept: '*/*' };
}

function ytId(u) {
  try {
    const url = new URL(u);
    if (url.hostname.includes('youtu.be')) return url.pathname.split('/')[1] || '';
    return url.searchParams.get('v') || '';
  } catch {
    return '';
  }
}

// Fast, keyless metadata for the info card (YouTube oEmbed). The card shows
// quality buttons; actual download links resolve only when one is picked.
export async function resolveInfo(shareUrl, ctx) {
  const id = ytId(shareUrl);
  let title = 'YouTube video';
  let author = '';
  try {
    const r = await fetch(
      `{{https://www.youtube.com/oembed?url=${encodeURIComponent(shareUrl}})}&format=json`,
      { signal: AbortSignal.timeout(Math.min(ctx.timeoutMs || 15000, 15000)) },
    );
    if (r.ok) {
      const j = await r.json();
      if (j.title) title = j.title;
      if (j.author_name) author = j.author_name;
    }
  } catch {}
  return {
    provider: name,
    share_url: shareUrl,
    final_url: shareUrl,
    surl: '',
    title,
    author,
    thumbnail: id ? `{{https://i.ytimg.com/vi/${id}}}/hqdefault.jpg` : '',
    qualities: QUALITIES,
    files: [], // no direct links until a quality is picked
  };
}

// Resolve one quality into a downloadable file entry (calls the hosted API).
export async function resolveQuality(shareUrl, format = '360', type = 'mp4', ctx = {}) {
  if (!YT_API_KEY) throw new Error('YouTube resolver API key is not configured');
  const fmt = type === 'mp3' ? YT_MP3_FORMAT : format;
  const api =
    `${YT_API_BASE}/download/ytdl?url=${encodeURIComponent(shareUrl)}` +
    `&format=${encodeURIComponent(fmt)}&type=${encodeURIComponent(type)}` +
    `&apikey=${encodeURIComponent(YT_API_KEY)}`;
  const resp = await fetch(api, {
    signal: AbortSignal.timeout(Math.max(ctx.timeoutMs || 0, 90_000)),
  });
  if (!resp.ok) throw new Error(`YouTube resolver failed (HTTP ${resp.status})`);
  const data = await resp.json().catch(() => null);
  if (!data?.status || !data?.data?.download) {
    throw new Error(
      `YouTube resolve failed: ${data?.err || data?.error || data?.message || 'no download link returned'}`,
    );
  }

  const d = data.data;
  const title = String(d.title || 'youtube-video').trim() || 'youtube-video';
  const ext = type === 'mp3' ? 'mp3' : 'mp4';

  // Learn the real file size with one HEAD request (non-fatal if it fails).
  let sizeBytes = 0;
  try {
    const head = await fetch(d.download, {
      method: 'HEAD',
      signal: AbortSignal.timeout(10_000),
    });
    const len = Number(head.headers.get('content-length') || 0);
    if (len > 0) sizeBytes = len;
  } catch {}

  const durationSec = Number(d.duration) || 0;
  const durationTxt = durationSec
    ? `${Math.floor(durationSec / 60)}:${String(Math.floor(durationSec % 60)).padStart(2, '0')} min`
    : '';

  const id = ytId(shareUrl);
  const safeTitle = title.replace(/[\\/:*?"<>|\r\n]+/g, '_').slice(0, 80) || 'youtube-video';
  const qualityLabel = type === 'mp3' ? 'MP3' : `${format}p`;

  return {
    name: `${safeTitle} [${qualityLabel}].${ext}`,
    size: sizeBytes ? formatSize(sizeBytes) : durationTxt || 'unknown size',
    size_bytes: sizeBytes,
    thumbnail: id ? `{{https://i.ytimg.com/vi/${id}}}/hqdefault.jpg` : '',
    dlink: d.download,
    is_dir: false,
    path: '',
    fs_id: '',
  };
}
