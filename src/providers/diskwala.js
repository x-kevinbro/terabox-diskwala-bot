import { formatSize } from '../utils.js';

export const name = 'diskwala';

export const hosts = new Set([
  'diskwala.com',
  'www.diskwala.com',
  'diskwala.net',
  'www.diskwala.net',
  'diskwala.app',
  'www.diskwala.app',
]);

const UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36';

const BROWSER_HEADERS = {
  'User-Agent': UA,
  Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
  'Accept-Language': 'en-US,en;q=0.9',
  Referer: 'https://diskwala.net/',
  DNT: '1',
};

export function downloadHeaders() {
  return {
    'User-Agent': UA,
    Accept: '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    Referer: 'https://diskwala.net/',
    DNT: '1',
  };
}

// Direct media URLs embedded in the page (plain or JSON-escaped).
const MEDIA_RE = /https?:\/\/[^\s"'<>\\]+?\.(?:mp4|mkv|webm|mov|m4v|mp3|zip|apk)(?:\?[^\s"'<>\\]*)?/gi;
// JSON properties commonly used by these pages, e.g. "downloadUrl": "https://...".
const PROP_RE =
  /"(?:downloadUrl|download_url|downloadLink|fileUrl|file_url|videoUrl|video_url|streamUrl|stream_url|src|url)"\s*:\s*"(https?:\/\/[^"]+)"/gi;

function clean(u) {
  return u.replace(/\\\//g, '/').replace(/&amp;/g, '&');
}

// EXPERIMENTAL: Diskwala has no documented public API, so this resolver loads
// the share page and scans it for direct media URLs. If Diskwala changes their
// page, adjust the patterns above — the Terabox resolver is the stable one.
export async function resolveInfo(shareUrl, ctx) {
  const timeout = AbortSignal.timeout(ctx.timeoutMs);
  const resp = await fetch(shareUrl, {
    headers: BROWSER_HEADERS,
    redirect: 'follow',
    signal: timeout,
  });
  if (!resp.ok) throw new Error(`Diskwala page fetch failed (HTTP ${resp.status})`);
  const html = await resp.text();

  const found = new Map();
  for (const m of html.matchAll(PROP_RE)) {
    const u = clean(m[1]);
    if (/\.(jpe?g|png|webp|gif|css|js)(\?|$)/i.test(u)) continue;
    found.set(u, u);
  }
  for (const m of html.matchAll(MEDIA_RE)) {
    found.set(clean(m[0]), clean(m[0]));
  }

  const urls = [...found.values()];
  if (!urls.length) {
    throw new Error(
      'Could not find a direct media link on this Diskwala page. The Diskwala resolver is experimental and the site layout may have changed — Terabox-family links are fully supported.',
    );
  }

  const files = urls.map((u, i) => {
    let base = '';
    try {
      base = decodeURIComponent(new URL(u).pathname.split('/').pop() || '');
    } catch {
      base = '';
    }
    return {
      name: base || `file_${i + 1}`,
      size: formatSize(0),
      size_bytes: 0,
      thumbnail: '',
      dlink: u,
      is_dir: false,
      path: '',
      fs_id: '',
    };
  });

  return {
    provider: name,
    share_url: shareUrl,
    final_url: resp.url,
    surl: '',
    title: '',
    files,
  };
}
