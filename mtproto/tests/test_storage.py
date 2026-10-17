import asyncio
import sqlite3
import sys
import tempfile
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from storage import Ledger, ReviewRequired, StorageError, TursoSQL, encode_value, link_identity, turso_endpoint
from archive_service import ArchiveService, file_slot
from resolvers import RemoteFile, Resolved

URL='https://teraboxshare.com/s/1example'


class SQLiteSQL:
    def __init__(self):
        self.connection=sqlite3.connect(':memory:')
        self.connection.row_factory=sqlite3.Row
    async def execute(self,sql,args=()):
        cursor=self.connection.execute(sql,args)
        rows=[dict(r) for r in cursor.fetchall()]
        self.connection.commit()
        return rows


class IdentityTests(unittest.TestCase):
    def test_mirror_share_forms_and_tracking_deduplicate(self):
        key,_=link_identity(URL)
        for url in ['http://www.terabox.com/s/1example#preview',
                    'https://1024terabox.com/s/1example?utm_source=chat',
                    'https://terabox.com/sharing/link?surl=example']:
            self.assertEqual(link_identity(url)[0],key)
    def test_case_and_password_are_not_discarded(self):
        self.assertNotEqual(link_identity(URL)[0],link_identity(URL.replace('example','Example'))[0])
        self.assertNotEqual(link_identity(URL)[0],link_identity(URL+'?pwd=secret')[0])
    def test_database_token_can_only_go_to_turso_https(self):
        self.assertEqual(turso_endpoint('libsql://demo-team.turso.io'),'https://demo-team.turso.io/v2/pipeline')
        for url in ['http://demo.turso.io','https://turso.io.evil.test','https://localhost',
                    'https://demo.turso.io:8443','https://demo.turso.io/path']:
            with self.assertRaises(StorageError):turso_endpoint(url)
    def test_bound_values(self):
        self.assertEqual(encode_value(99),{'type':'integer','value':'99'})
        self.assertEqual(encode_value(None),{'type':'null'})


class LedgerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.db=SQLiteSQL();self.ledger=Ledger(self.db);await self.ledger.initialize()
    async def asyncTearDown(self):self.db.connection.close()
    async def test_save_all_links_before_worker_and_deduplicate_across_users(self):
        rows=await self.ledger.enqueue([URL,URL,'https://terabox.com/s/1other'],100,'https://graph.org/article')
        self.assertEqual(len(rows),2)
        self.assertTrue(all(kind=='new' for _,kind in rows))
        again=await self.ledger.enqueue(['https://terabox.com/s/1example'],200)
        self.assertEqual(again[0][1],'duplicate')
        self.assertEqual(again[0][0]['chat_id'],100)
        self.assertEqual(len(again[0][0]['token']),32)
        self.assertEqual(len(await self.db.execute('SELECT * FROM archive_links')),2)
    async def test_atomic_claim_and_global_single_worker(self):
        await self.ledger.enqueue([URL,URL+'2'],100)
        claims=await asyncio.gather(self.ledger.claim('one'),self.ledger.claim('two'))
        self.assertEqual(sum(c is not None for c in claims),1)
        first=next(c for c in claims if c)
        await self.ledger.finish(first['id'],first['owner'],'completed')
        self.assertIsNotNone(await self.ledger.claim('two'))
    async def test_completed_link_is_never_requeued(self):
        await self.ledger.enqueue([URL],100);job=await self.ledger.claim('one')
        await self.ledger.finish(job['id'],'one','completed')
        again=await self.ledger.enqueue([URL],200)
        self.assertEqual(again[0][1],'duplicate');self.assertEqual(again[0][0]['state'],'completed')
        self.assertIsNone(await self.ledger.claim('two'))
    async def test_failed_link_retries_but_keeps_its_token(self):
        initial=(await self.ledger.enqueue([URL],100))[0][0]
        job=await self.ledger.claim('one');await self.ledger.finish(job['id'],'one','failed')
        retry=(await self.ledger.enqueue([URL],200))[0]
        self.assertEqual(retry[1],'retry');self.assertEqual(retry[0]['token'],initial['token'])
        self.assertEqual(retry[0]['chat_id'],200)
    async def test_expired_jobs_require_review_not_reupload(self):
        await self.ledger.enqueue([URL],100);job=await self.ledger.claim('one')
        await self.db.execute('UPDATE archive_links SET lease_until=0 WHERE id=?',(job['id'],))
        await self.ledger.expire()
        self.assertEqual((await self.ledger.enqueue([URL],100))[0][0]['state'],'review')
        self.assertIsNone(await self.ledger.claim('two'))
        with self.assertRaises(ReviewRequired):await self.ledger.heartbeat(job['id'],'one')
    async def test_same_content_reuses_one_storage_message(self):
        rows=await self.ledger.enqueue([URL,URL+'2'],100)
        a,fresh=await self.ledger.reserve_asset('digest',rows[0][0]['id'],'slot','a.mp4',123)
        self.assertTrue(fresh)
        a=await self.ledger.save_asset('digest',rows[0][0]['id'],-100123,42,'file-id')
        other,fresh=await self.ledger.reserve_asset('digest',rows[1][0]['id'],'slot','b.mp4',123)
        self.assertFalse(fresh);self.assertEqual(other['message_id'],42)
        _,assets=await self.ledger.retrieval(rows[1][0]['token'])
        self.assertEqual(assets,[{'channel_id':-100123,'message_id':42}])
    async def test_unconfirmed_upload_cannot_be_claimed_again(self):
        rows=await self.ledger.enqueue([URL,URL+'2'],100)
        await self.ledger.reserve_asset('digest',rows[0][0]['id'],'slot','a',1)
        a,fresh=await self.ledger.reserve_asset('digest',rows[1][0]['id'],'slot','a',1)
        self.assertFalse(fresh);self.assertEqual(a['state'],'uploading')
    async def test_cancel_and_invalid_retrieval(self):
        await self.ledger.enqueue([URL],100)
        self.assertEqual(len(await self.ledger.cancel_queued(200)),0)
        self.assertEqual(len(await self.ledger.cancel_queued(100)),1)
        self.assertIsNone(await self.ledger.claim('one'))
        self.assertEqual(await self.ledger.retrieval("'; DROP TABLE archive_links;--"),(None,[]))


@asynccontextmanager
async def slot():yield


class ArchiveTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory();self.db=SQLiteSQL();self.ledger=Ledger(self.db)
        await self.ledger.initialize()
        self.status=SimpleNamespace(edit_text=AsyncMock())
        self.client=SimpleNamespace(send_message=AsyncMock(return_value=self.status),copy_message=AsyncMock(),
            send_video=AsyncMock(return_value=SimpleNamespace(chat=SimpleNamespace(id=-100123),id=42,
                        video=SimpleNamespace(file_id='id'),document=None)))
        self.config=SimpleNamespace(storage_channel_id=-100123,max_bytes=10000,request_timeout=1)
        self.service=ArchiveService(self.ledger,self.client,None,self.config,slot,'delivery_test_bot')
        await self.ledger.enqueue([URL],100);self.job=await self.ledger.claim(self.service.owner)
        self.item=RemoteFile('a.mp4','https://cdn.example/file',3)
        self.result=Resolved('terabox',URL,'a',[self.item])
    async def asyncTearDown(self):self.db.connection.close();self.temp.cleanup()
    async def download(self,*args,**kwargs):
        path=Path(self.temp.name)/'file.mp4';path.write_bytes(b'abc');return path
    async def test_upload_channel_then_persist_then_copy_user_and_cleanup(self):
        with patch('archive_service.download_to_disk',side_effect=self.download):
            asset,copied=await self.service.archive_file(self.job,self.result,self.item,0)
        self.client.send_video.assert_awaited_once();self.client.copy_message.assert_awaited_once_with(100,-100123,42)
        self.assertEqual(asset['state'],'stored');self.assertTrue(copied)
        self.assertEqual(list(Path(self.temp.name).iterdir()),[])
        with patch('archive_service.download_to_disk',AsyncMock()) as download:
            await self.service.archive_file(self.job,self.result,self.item,0)
            download.assert_not_awaited()
        self.assertEqual(self.client.send_video.await_count,1)
    async def test_identical_content_in_different_share_does_not_upload_twice(self):
        with patch('archive_service.download_to_disk',side_effect=self.download):
            await self.service.archive_file(self.job,self.result,self.item,0)
            await self.ledger.finish(self.job['id'],self.service.owner,'completed')
            await self.ledger.enqueue([URL+'2'],200)
            other=await self.ledger.claim(self.service.owner)
            await self.service.archive_file(other,self.result,self.item,0)
        self.assertEqual(self.client.send_video.await_count,1)
        self.assertEqual(self.client.copy_message.await_count,2)
    async def test_upload_timeout_requires_review_and_no_second_upload(self):
        self.client.send_video.side_effect=TimeoutError()
        with patch('archive_service.download_to_disk',side_effect=self.download):
            with self.assertRaises(ReviewRequired):await self.service.archive_file(self.job,self.result,self.item,0)
        with patch('archive_service.download_to_disk',AsyncMock()) as download:
            with self.assertRaises(ReviewRequired):await self.service.archive_file(self.job,self.result,self.item,0)
            download.assert_not_awaited()
        self.assertEqual(self.client.send_video.await_count,1)
    async def test_failed_database_confirmation_never_uploads_again(self):
        with patch('archive_service.download_to_disk',side_effect=self.download),patch.object(self.ledger,'save_asset',AsyncMock(side_effect=StorageError('offline'))):
            with self.assertRaises(ReviewRequired):await self.service.archive_file(self.job,self.result,self.item,0)
        with self.assertRaises(ReviewRequired):await self.service.archive_file(self.job,self.result,self.item,0)
        self.assertEqual(self.client.send_video.await_count,1)
    async def test_user_copy_failure_does_not_lose_stored_video(self):
        self.client.copy_message.side_effect=RuntimeError('blocked')
        with patch('archive_service.download_to_disk',side_effect=self.download):
            asset,copied=await self.service.archive_file(self.job,self.result,self.item,0)
        self.assertFalse(copied);self.assertEqual(asset['state'],'stored')
        _,assets=await self.ledger.retrieval(self.job['token']);self.assertEqual(len(assets),1)
    async def test_duplicate_submission_links_to_companion_not_channel(self):
        await self.ledger.finish(self.job['id'],self.service.owner,'completed')
        msg=SimpleNamespace(chat=SimpleNamespace(id=200),reply_text=AsyncMock())
        await self.service.submit(msg,[URL])
        first=msg.reply_text.await_args_list[0]
        self.assertIn('Already processed',first.args[0])
        self.assertEqual(first.kwargs['reply_markup'].inline_keyboard[0][0].url,
            'https://t.me/delivery_test_bot?start='+self.job['token'])
    async def test_queued_duplicate_gets_no_companion_button(self):
        msg=SimpleNamespace(chat=SimpleNamespace(id=200),reply_text=AsyncMock())
        await self.service.submit(msg,[URL])
        first=msg.reply_text.await_args_list[0]
        self.assertIn('Already queued',first.args[0]);self.assertNotIn('reply_markup',first.kwargs)
    async def test_new_video_is_sent_directly_without_companion_button(self):
        with patch('archive_service.resolve',AsyncMock(return_value=self.result)),\
             patch('archive_service.download_to_disk',side_effect=self.download):
            await self.service.process(self.job)
        self.client.copy_message.assert_awaited_once_with(100,-100123,42)
        texts=[c.args[1] for c in self.client.send_message.await_args_list]
        self.assertEqual(len(texts),1);self.assertIn('Starting download',texts[0])
        self.assertNotIn('reply_markup',self.client.send_message.await_args_list[0].kwargs)
        self.assertIn('Sent!',self.status.edit_text.await_args_list[-1].args[0])
    async def test_failed_link_gets_one_short_reply_without_log(self):
        with patch('archive_service.resolve',AsyncMock(side_effect=RuntimeError('down'))):
            await self.service.process(self.job)
        texts=[c.args[1] for c in self.client.send_message.await_args_list]
        self.assertEqual(texts,['\u274c Could not get the video from this link.'])
    async def test_new_link_while_idle_sends_no_database_log(self):
        await self.ledger.finish(self.job['id'],self.service.owner,'completed')
        msg=SimpleNamespace(chat=SimpleNamespace(id=200),reply_text=AsyncMock())
        await self.service.submit(msg,['https://teraboxshare.com/s/1newlinkabc'])
        msg.reply_text.assert_not_awaited()
    async def test_worker_shutdown_cancels_active_transfer_without_orphan_task(self):
        await self.ledger.finish(self.job['id'],self.service.owner,'cancelled')
        await self.ledger.enqueue([URL],100)
        started=asyncio.Event();stopped=asyncio.Event()
        async def process(_):
            started.set()
            try:await asyncio.Event().wait()
            finally:stopped.set()
        with patch.object(self.service,'process',side_effect=process):
            worker=asyncio.create_task(self.service.run())
            await asyncio.wait_for(started.wait(),1)
            worker.cancel();await asyncio.gather(worker,return_exceptions=True)
            self.assertTrue(stopped.is_set())
            self.assertIsNone(self.service.active_task)
    async def test_queue_resumes_persisted_job_after_service_restart(self):
        await self.ledger.finish(self.job['id'],self.service.owner,'cancelled')
        await self.ledger.enqueue([URL],100)
        replacement=ArchiveService(self.ledger,self.client,None,self.config,slot,'delivery_test_bot')
        self.assertEqual((await self.ledger.claim(replacement.owner))['id'],self.job['id'])


class HttpTests(unittest.IsolatedAsyncioTestCase):
    async def test_parameter_binding_headers_and_error_redaction(self):
        class Response:
            status=200
            async def __aenter__(self):return self
            async def __aexit__(self,*args):pass
            async def json(self):return {'results':[{'type':'ok','response':{'result':{
                'cols':[{'name':'total'}],'rows':[[{'type':'integer','value':'7'}]]}}}]}
        response=Response()
        session=SimpleNamespace(post=lambda *a,**kw:response)
        client=TursoSQL(session,'libsql://example-team.turso.io','test-secret')
        self.assertEqual(await client.execute('SELECT ? AS total',(7,)),[{'total':7}])
        response.status=401
        with self.assertRaises(StorageError) as caught:await client.execute('SELECT 1')
        self.assertNotIn('test-secret',str(caught.exception))


class DeliveryBotTests(unittest.IsolatedAsyncioTestCase):
    async def test_second_bot_copies_channel_message_and_never_downloads(self):
        import delivery_bot
        handlers=[]
        client=SimpleNamespace(copy_message=AsyncMock(),on_message=lambda *_:lambda fn:handlers.append(fn) or fn)
        ledger=SimpleNamespace(retrieval=AsyncMock(return_value=({'state':'completed'},[{'channel_id':-100123,'message_id':42}])))
        config=SimpleNamespace(api_id=1,api_hash='test-only',delivery_bot_token='test-only',public_bot=True,allowed_users=set())
        msg=SimpleNamespace(from_user=SimpleNamespace(id=100),chat=SimpleNamespace(id=100),
                            text='/start '+'a'*32,reply_text=AsyncMock())
        with patch.object(delivery_bot,'Client',return_value=client):
            delivery_bot.create_delivery_bot(config,ledger)
        await handlers[0](client,msg)
        client.copy_message.assert_awaited_once_with(100,-100123,42)
        self.assertIn('Sent: 1',msg.reply_text.await_args.args[0])
    async def test_invalid_token_does_not_expose_channel_or_send_media(self):
        import delivery_bot
        handlers=[]
        client=SimpleNamespace(copy_message=AsyncMock(),on_message=lambda *_:lambda fn:handlers.append(fn) or fn)
        ledger=SimpleNamespace(retrieval=AsyncMock(return_value=(None,[])))
        config=SimpleNamespace(api_id=1,api_hash='test-only',delivery_bot_token='test-only',public_bot=True,allowed_users=set())
        msg=SimpleNamespace(from_user=SimpleNamespace(id=100),chat=SimpleNamespace(id=100),
                            text='/start invalid',reply_text=AsyncMock())
        with patch.object(delivery_bot,'Client',return_value=client):delivery_bot.create_delivery_bot(config,ledger)
        await handlers[0](client,msg)
        client.copy_message.assert_not_awaited()
        self.assertIn('invalid',msg.reply_text.await_args.args[0])
