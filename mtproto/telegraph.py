"""Read public Telegraph articles; never fetch arbitrary user-supplied hosts."""
from __future__ import annotations

from html import unescape
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit, urlunsplit

import aiohttp

from resolvers import URL_RE, pick_provider

PAGE_HOSTS = {"graph.org", "www.graph.org", "telegra.ph", "www.telegra.ph"}
MAX_PAGE_BYTES = 1024 * 1024
MAX_PAGES = 5
MAX_LINKS = 50


class PageError(Exception):
    pass


def page_url(raw: str) -> str | None:
    try:
        url = urlsplit(raw)
        if (url.scheme not in {"http", "https"} or url.hostname not in PAGE_HOSTS
                or url.username or url.password or url.port not in {None, 443}
                or url.path in {"", "/"}):
            return None
        return urlunsplit(("https", url.hostname, url.path, url.query, ""))
    except ValueError:
        return None


def page_links(text: str) -> list[str]:
    found = list(dict.fromkeys(url for raw in URL_RE.findall(text or "") if (url := page_url(raw))))
    if len(found) > MAX_PAGES:
        raise PageError(f"Send at most {MAX_PAGES} Graph/Telegraph pages in one batch.")
    return found


def normalize_share(raw: str) -> str | None:
    try:
        raw = unescape(raw).strip().rstrip(".,;!")
        url = urlsplit(raw)
        if (len(raw) > 4096 or url.scheme not in {"http", "https"}
                or url.username or url.password or url.port not in {None, 443}
                or not pick_provider(raw)):
            return None
        return urlunsplit(("https", url.hostname.lower(), url.path, url.query, ""))
    except ValueError:
        return None


class ArticleParser(HTMLParser):
    def __init__(self, base: str):
        super().__init__(convert_charrefs=True)
        self.base = base
        self.article = 0
        self.found_article = False
        self.hidden = 0
        self.links: list[str] = []
        self.seen: set[str] = set()

    def add(self, raw: str) -> None:
        url = normalize_share(urljoin(self.base, raw))
        if url and url not in self.seen:
            if len(self.links) >= MAX_LINKS:
                raise PageError(f"This article exceeds the {MAX_LINKS}-link batch limit. Split it into smaller pages.")
            self.seen.add(url)
            self.links.append(url)

    def handle_starttag(self, tag, attrs):
        if tag == "article":
            self.article += 1
            self.found_article = True
        if self.article and tag in {"script", "style", "noscript"}:
            self.hidden += 1
        if self.article and not self.hidden and tag == "a":
            href = dict(attrs).get("href")
            if href:
                self.add(href)

    def handle_endtag(self, tag):
        if self.article and tag in {"script", "style", "noscript"}:
            self.hidden = max(0, self.hidden - 1)
        if tag == "article":
            self.article = max(0, self.article - 1)

    def handle_data(self, data):
        if self.article and not self.hidden:
            for raw in URL_RE.findall(data):
                self.add(raw)


def parse_article(html: str, base: str) -> list[str]:
    parser = ArticleParser(base)
    parser.feed(html)
    if not parser.found_article:
        raise PageError("No public Telegraph article was found on this page.")
    return parser.links


async def fetch_article(session: aiohttp.ClientSession, raw: str, timeout: int = 30) -> list[str]:
    target = page_url(raw)
    if not target:
        raise PageError("Use a public graph.org or telegra.ph article URL.")
    try:
        # Validate every redirect before following it; no cookies or auth are sent.
        for _ in range(4):
            async with session.get(target, allow_redirects=False,
                    headers={"User-Agent": "Diskwala-Terabox-Bot/graph-batch", "Accept": "text/html"},
                    timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                if response.status in {301, 302, 303, 307, 308}:
                    location = response.headers.get("Location")
                    next_url = page_url(urljoin(target, location)) if location else None
                    if not next_url:
                        raise PageError("The article redirected outside Graph/Telegraph; refused.")
                    target = next_url
                    continue
                if response.status != 200:
                    raise PageError(f"Article could not be loaded (HTTP {response.status}).")
                content_type = response.headers.get("Content-Type", "").lower()
                if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
                    raise PageError("The page did not return an HTML article.")
                chunks = bytearray()
                async for chunk in response.content.iter_chunked(16 * 1024):
                    chunks.extend(chunk)
                    if len(chunks) > MAX_PAGE_BYTES:
                        raise PageError("The article is too large (maximum 1 MB).")
                return parse_article(chunks.decode("utf-8", errors="replace"), target)
        raise PageError("The article redirected too many times.")
    except (aiohttp.ClientError, TimeoutError) as exc:
        raise PageError("The article could not be reached. Try again later.") from exc
