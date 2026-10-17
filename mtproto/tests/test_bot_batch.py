import asyncio
import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
_TEMP=tempfile.TemporaryDirectory()
os.environ.update(API_ID='1', API_HASH='test-only-api-hash-not-valid', BOT_TOKEN='12345:test-only-not-valid', SESSION_DIR=_TEMP.name, DOWNLOAD_DIR=_TEMP.name, PUBLIC_BOT='true')
import bot
bot.LOOP.run_until_complete(asyncio.sleep(0))
from resolvers import RemoteFile, Resolved
from downloader import DownloadError


def message(chat=100):
    status=SimpleNamespace(edit_text=AsyncMock())
    return SimpleNamespace(chat=SimpleNamespace(id=chat),from_user=SimpleNamespace(id=chat),
        text='https://graph.org/test',caption=None,entities=None,caption_entities=None,
        reply_text=AsyncMock(return_value=status)),status


class IntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        bot.job_slot=asyncio.Semaphore(1);bot.batch_lock=asyncio.Lock();bot.batch_jobs.clear()
        bot.health.STATE.update(active_jobs=0,queued_jobs=0,pending_batches=0)
    async def asyncTearDown(self):
        tasks=list(bot.batch_jobs.values())
        for task in tasks:task.cancel()
        if tasks:await asyncio.gather(*tasks,return_exceptions=True)
        bot.batch_jobs.clear()
    async def test_graph_handler_starts_background_task_and_limits_duplicate_batches(self):
        msg,_=message()
        block=asyncio.Event()
        async def process(*_):await block.wait()
        with patch.object(bot,'process_pages',side_effect=process):
            await bot.on_link(None,msg)
            self.assertEqual(len(bot.batch_jobs),1)
            await bot.on_link(None,msg)
            self.assertIn('already',msg.reply_text.await_args.args[0])
            await bot.on_cancel(None,msg)
            await asyncio.sleep(0);await asyncio.sleep(0)
            self.assertEqual(bot.batch_jobs,{})
            self.assertEqual(bot.health.STATE['pending_batches'],0)
    async def test_plain_share_links_keep_existing_button_behavior(self):
        msg,status=message();msg.text='https://teraboxshare.com/s/example'
        result=Resolved('terabox',msg.text,'example',[RemoteFile('a.mp4','https://cdn.example/a')])
        with patch.object(bot,'resolve',AsyncMock(return_value=result)),patch.object(bot,'deliver',AsyncMock()) as delivery:
            await bot.on_link(None,msg)
            self.assertIsNotNone(status.edit_text.await_args.kwargs.get('reply_markup'))
            delivery.assert_not_awaited()
    async def test_hidden_telegram_link_is_detected(self):
        msg,_=message();msg.text='Watch this page';msg.entities=[SimpleNamespace(url='https://graph.org/test')]
        self.assertIn('https://graph.org/test',bot.message_link_text(msg))
    async def test_page_batch_deduplicates_and_processes_all_seven_links(self):
        msg,status=message()
        links=[f'https://teraboxshare.com/s/{i}' for i in range(7)]
        result=Resolved('terabox','x','title',[RemoteFile('a.mp4','https://cdn.example/a')])
        with patch.object(bot,'fetch_article',AsyncMock(return_value=links+[links[0]])),patch.object(bot,'resolve',AsyncMock(return_value=result)),patch.object(bot,'deliver',AsyncMock(return_value='sent')) as delivery:
            await bot.process_pages(msg,['https://graph.org/test'],[])
            self.assertEqual(delivery.await_count,7)
            self.assertIn('Sent: 7',status.edit_text.await_args.args[0])
    async def test_slot_counters_reset_after_success_and_cancelled_waiter(self):
        gate=asyncio.Event()
        async def transfer():
            async with bot.transfer_slot():await gate.wait()
        a=asyncio.create_task(transfer());await asyncio.sleep(0)
        b=asyncio.create_task(transfer());await asyncio.sleep(0)
        self.assertEqual(bot.health.STATE['active_jobs'],1)
        self.assertEqual(bot.health.STATE['queued_jobs'],1)
        b.cancel();await asyncio.gather(b,return_exceptions=True)
        gate.set();await a
        self.assertEqual(bot.health.STATE['active_jobs'],0)
        self.assertEqual(bot.health.STATE['queued_jobs'],0)
        self.assertEqual(bot.job_slot._value,1)
    async def test_delivery_sends_to_chat_and_cleans_disk(self):
        msg,_=message();file=RemoteFile('test.bin','https://cdn.example/a',3)
        result=Resolved('terabox','https://teraboxshare.com/s/a','a',[file])
        path=Path(_TEMP.name)/'transfer.bin';path.write_bytes(b'abc')
        with patch.object(bot,'resolve',AsyncMock(return_value=result)),patch.object(bot,'download_to_disk',AsyncMock(return_value=path)),patch.object(bot.app,'send_document',AsyncMock()) as sent:
            self.assertEqual(await bot.deliver(msg,result,file,0),'sent')
            self.assertEqual(sent.await_args.args[0],msg.chat.id)
        self.assertFalse(path.exists());self.assertEqual(bot.health.STATE['active_jobs'],0)
    async def test_oversized_file_skips_without_download(self):
        msg,_=message();file=RemoteFile('large.bin','https://cdn.example/a',bot.config.max_bytes+1)
        with patch.object(bot,'download_to_disk',AsyncMock()) as download:
            self.assertEqual(await bot.deliver(msg,Resolved('terabox','x','x',[file]),file,0),'skipped')
            download.assert_not_awaited()
    async def test_transfer_exception_still_releases_slot(self):
        msg,_=message();file=RemoteFile('file.bin','https://cdn.example/a',3);result=Resolved('terabox','x','x',[file])
        with patch.object(bot,'resolve',AsyncMock(return_value=result)),patch.object(bot,'download_to_disk',AsyncMock(side_effect=DownloadError('network error'))):
            self.assertEqual(await bot.deliver(msg,result,file,0),'failed')
        self.assertEqual(bot.health.STATE['active_jobs'],0);self.assertEqual(bot.job_slot._value,1)


def tearDownModule():
    bot.LOOP.close()
    _TEMP.cleanup()


class DownloaderTests(unittest.IsolatedAsyncioTestCase):
    async def test_cancelled_and_timed_out_streams_remove_partial_files(self):
        import downloader
        for error in (asyncio.CancelledError(), TimeoutError()):
            async def chunks(_size):
                yield b'partial file'
                raise error
            response=SimpleNamespace(status=200,headers={},content=SimpleNamespace(iter_chunked=chunks))
            class Context:
                def __getattr__(self,name):return getattr(response,name)
                async def __aenter__(self):return response
                async def __aexit__(self,*_):pass
            session=SimpleNamespace(get=AsyncMock(return_value=Context()))
            expected=asyncio.CancelledError if isinstance(error,asyncio.CancelledError) else DownloadError
            with self.assertRaises(expected):
                await downloader.download_to_disk(session,'https://cdn.example/file',{},'file.bin')
            self.assertEqual(list(Path(_TEMP.name).glob('dl_*')),[])
