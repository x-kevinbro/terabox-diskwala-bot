import asyncio
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from telegraph import ArticleParser, MAX_LINKS, PageError, fetch_article, page_links, page_url, parse_article
from page_batch import BatchStats, run_batch

URL = "https://graph.org/test-page"
SHARE = "https://teraboxshare.com/s/1example"


class Stream:
    def __init__(self, chunks): self.chunks = chunks
    async def iter_chunked(self, _size):
        for chunk in self.chunks: yield chunk


class Response:
    def __init__(self, body=b"", status=200, headers=None, chunks=None):
        self.status = status
        self.headers = headers if headers is not None else {"Content-Type": "text/html"}
        self.content = Stream(chunks if chunks is not None else [body])
    async def __aenter__(self): return self
    async def __aexit__(self, *_): pass


class Session:
    def __init__(self, *responses): self.responses = list(responses); self.calls = []
    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


class ParserTests(unittest.TestCase):
    def test_anchor_and_plain_links_are_deduplicated_in_article_order(self):
        other = "https://teraboxshare.com/s/1second"
        body = f'<a href="https://teraboxshare.com/s/footer">outside</a><article><a href="{SHARE}#preview">Watch</a>{SHARE}<p>{other}</p><a href="https://diskwala.com/app/demo?a=1&amp;b=2">file</a></article>'
        self.assertEqual(parse_article(body, URL), [SHARE, other, "https://diskwala.com/app/demo?a=1&b=2"])
    def test_script_style_and_unrelated_hosts_are_not_downloaded(self):
        body = f'<article><script>{SHARE}</script><style>{SHARE}</style><a href="https://evil.example/file">x</a><a href="https://graph.org/other">nested page</a></article>'
        self.assertEqual(parse_article(body, URL), [])
    def test_no_article_is_rejected(self):
        with self.assertRaises(PageError): parse_article(f'<body>{SHARE}</body>', URL)
    def test_page_allowlist_rejects_spoofed_or_unsafe_destinations(self):
        for url in ["https://graph.org.evil.com/article", "https://user:secret@graph.org/article", "https://graph.org:8443/a", "http://127.0.0.1/a", "file:///a", "https://graph.org/"]:
            self.assertIsNone(page_url(url), url)
        self.assertEqual(page_url("http://graph.org/test#fragment"), "https://graph.org/test")
    def test_multiple_pages_preserve_order_and_have_explicit_limit(self):
        self.assertEqual(page_links(f'{URL} https://telegra.ph/second {URL}'), [URL, "https://telegra.ph/second"])
        with self.assertRaises(PageError): page_links(" ".join(f'https://graph.org/a{i}' for i in range(6)))
    def test_link_cap_is_reported_not_silently_truncated(self):
        with self.assertRaises(PageError): parse_article('<article>'+" ".join(f'https://teraboxshare.com/s/{i}' for i in range(MAX_LINKS+1))+'</article>', URL)


class FetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_only_html_without_auth_or_automatic_redirects(self):
        s = Session(Response(f'<article>{SHARE}</article>'.encode()))
        self.assertEqual(await fetch_article(s, URL), [SHARE])
        _, args = s.calls[0]
        self.assertFalse(args['allow_redirects'])
        self.assertNotIn('Authorization', args['headers'])
        self.assertNotIn('Cookie', args['headers'])
    async def test_same_service_redirect_is_validated(self):
        s = Session(Response(status=302,headers={'Location':'https://telegra.ph/article'}),Response(f'<article>{SHARE}</article>'.encode()))
        self.assertEqual(await fetch_article(s, URL), [SHARE])
        self.assertEqual(s.calls[1][0], 'https://telegra.ph/article')
    async def test_external_redirect_is_never_followed(self):
        s = Session(Response(status=302,headers={'Location':'http://169.254.169.254/latest'}))
        with self.assertRaises(PageError): await fetch_article(s, URL)
        self.assertEqual(len(s.calls),1)
    async def test_oversized_pages_and_non_html_are_rejected(self):
        for response in [Response(chunks=[b'x'*(1024*1024), b'x']), Response(headers={'Content-Type':'application/json'}), Response(status=403)]:
            with self.assertRaises(PageError): await fetch_article(Session(response),URL)
    async def test_redirect_loop_is_bounded(self):
        s=Session(*(Response(status=302,headers={'Location':URL}) for _ in range(4)))
        with self.assertRaises(PageError): await fetch_article(s,URL)
        self.assertEqual(len(s.calls),4)


class BatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_resolve_download_upload_are_sequential_and_failures_continue(self):
        events=[]
        async def resolver(url):
            events.append('resolve:'+url)
            if url=='bad': raise RuntimeError('expired')
            return SimpleNamespace(files=[SimpleNamespace(is_dir=False,dlink=url,name=url+str(i)) for i in range(2)])
        async def deliver(result,item,index):
            events.append('start:'+item.name)
            await asyncio.sleep(0)
            events.append('end:'+item.name)
            return 'sent' if index==0 else 'skipped'
        stats=await run_batch(['first','bad','last'],resolver,deliver,AsyncMock())
        self.assertEqual((stats.sent,stats.skipped,stats.link_failures),(2,2,1))
        self.assertEqual(events,['resolve:first','start:first0','end:first0','start:first1','end:first1','resolve:bad','resolve:last','start:last0','end:last0','start:last1','end:last1'])
    async def test_delivery_failure_does_not_prevent_next_file(self):
        resolver=AsyncMock(return_value=SimpleNamespace(files=[SimpleNamespace(is_dir=False,dlink='x')]*2))
        stats=await run_batch(['x'],resolver,AsyncMock(side_effect=[RuntimeError('upload failed'),'sent']),AsyncMock())
        self.assertEqual((stats.failed,stats.sent),(1,1))
    async def test_cancellation_stops_the_batch(self):
        resolver=AsyncMock(return_value=SimpleNamespace(files=[SimpleNamespace(is_dir=False,dlink='x')]))
        delivery=AsyncMock(side_effect=asyncio.CancelledError)
        with self.assertRaises(asyncio.CancelledError): await run_batch(['a','b'],resolver,delivery,AsyncMock())
        self.assertEqual(resolver.await_count,1)
    async def test_file_limit_does_not_start_another_download(self):
        resolver=AsyncMock(return_value=SimpleNamespace(files=[SimpleNamespace(is_dir=False,dlink='x')]*3))
        delivery=AsyncMock(return_value='sent')
        with patch('page_batch.MAX_FILES',2): stats=await run_batch(['a','b'],resolver,delivery,AsyncMock())
        self.assertTrue(stats.limited); self.assertEqual(delivery.await_count,2); self.assertEqual(resolver.await_count,1)
